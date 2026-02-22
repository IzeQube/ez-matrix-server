import asyncio
import json
import os
import threading
import time

import serial
import paho.mqtt.client as mqtt
from fastapi import FastAPI
from pydantic import BaseModel, Field
from typing import Any, Dict

app = FastAPI()

# --- Global In-Memory State ---
device_state = {
    "device_status": "Offline",
    "cascade_mode": "Unknown",
    "outputs": {
        "output_1_source": "Unknown",
        "output_2_source": "Unknown"
    },
    "inputs_edid_index": {
        "input_1_edid_index": "Unknown",
        "input_2_edid_index": "Unknown",
        "input_3_edid_index": "Unknown",
        "input_4_edid_index": "Unknown"
    }
}

# --- Helpers for Environment Parsing ---
def _int_env(key: str, default: int) -> int:
    raw_value = os.getenv(key)
    if raw_value is None:
        return default
    try:
        return int(raw_value)
    except ValueError:
        print(f"Invalid int for {key}: {raw_value}, defaulting to {default}")
        return default


def _float_env(key: str, default: float) -> float:
    raw_value = os.getenv(key)
    if raw_value is None:
        return default
    try:
        return float(raw_value)
    except ValueError:
        print(f"Invalid float for {key}: {raw_value}, defaulting to {default}")
        return default


# --- Serial Connection Manager ---
class SerialConnectionManager:
    """Manages serial connection with automatic reconnection support."""
    
    def __init__(self, port: str, baudrate: int, timeout: float):
        self.port = port
        self.baudrate = baudrate
        self.timeout = timeout
        self.ser: serial.Serial | None = None
        self._lock = threading.Lock()
        self._is_connected = False
        self._last_error = None
    
    def connect(self) -> bool:
        """Attempts to connect to serial port. Returns True if successful."""
        with self._lock:
            try:
                if self.ser and self.ser.is_open:
                    self.ser.close()
                
                self.ser = serial.Serial(self.port, baudrate=self.baudrate, timeout=self.timeout)
                self._is_connected = True
                self._last_error = None
                print(f"✓ Serial connected: {self.port}")
                return True
            except Exception as e:
                self._is_connected = False
                self._last_error = str(e)
                self.ser = None
                print(f"✗ Serial connection failed: {e}")
                return False
    
    def disconnect(self):
        """Safely closes serial connection."""
        with self._lock:
            try:
                if self.ser and self.ser.is_open:
                    self.ser.close()
                    print(f"Serial disconnected: {self.port}")
            except Exception as e:
                print(f"Error during disconnect: {e}")
            finally:
                self._is_connected = False
                self.ser = None
    
    def is_connected(self) -> bool:
        """Returns connection status."""
        with self._lock:
            return self._is_connected and self.ser is not None and self.ser.is_open
    
    def write(self, command: str) -> bool:
        """Thread-safe write to serial port. Returns True if successful."""
        with self._lock:
            if not self._is_connected or not self.ser or not self.ser.is_open:
                return False
            try:
                print(f"TX: {command}")
                self.ser.write(f"{command}\r".encode('utf-8'))
                return True
            except Exception as e:
                print(f"Serial Write Error: {e}")
                self._is_connected = False
                return False
    
    def readline(self) -> bytes:
        """Thread-safe read from serial port. May raise exceptions."""
        with self._lock:
            if not self.ser or not self.ser.is_open:
                raise IOError("Serial port not open")
            return self.ser.readline()
    
    def get_status(self) -> str:
        """Returns human-readable status."""
        if self.is_connected():
            return "Online"
        elif self._last_error:
            return f"Error: {self._last_error}"
        else:
            return "Reconnecting..."


# --- Configuration ---
MQTT_BROKER = os.getenv("MQTT_BROKER", "192.168.60.16")
MQTT_PORT = _int_env("MQTT_PORT", 1883)
MQTT_TOPIC_STATUS = os.getenv("MQTT_TOPIC_STATUS", "serial/status")
MQTT_TOPIC_CONTROL_ROOT = os.getenv("MQTT_TOPIC_CONTROL_ROOT", "serial/control")
MQTT_TOPIC_DEVICE_AVAILABLE = os.getenv("MQTT_TOPIC_DEVICE_AVAILABLE", "serial/device/available")
SERIAL_PORT = os.getenv("SERIAL_PORT", "/dev/ttyUSB0")
SERIAL_BAUDRATE = _int_env("SERIAL_BAUDRATE", 57600)
SERIAL_TIMEOUT = _float_env("SERIAL_TIMEOUT", 1.0)
SERIAL_RECONNECT_INTERVAL = _float_env("SERIAL_RECONNECT_INTERVAL", 60.0)

# --- Serial & Locking ---
serial_message_queue: asyncio.Queue[str] = asyncio.Queue()
serial_manager = SerialConnectionManager(SERIAL_PORT, SERIAL_BAUDRATE, SERIAL_TIMEOUT)

# Initial connection attempt
if serial_manager.connect():
    device_state["device_status"] = "Online"
else:
    device_state["device_status"] = "Reconnecting..."

# --- Helper: Thread-Safe Serial Write ---
def safe_serial_write(command: str):
    """Writes to serial port using the connection manager."""
    if not serial_manager.write(command):
        print(f"Command ignored (not connected): {command}")

# --- MQTT Callbacks ---

def on_connect(client, userdata, flags, rc):
    """Subscribes to control topics upon connection."""
    print(f"Connected to MQTT Broker with result code {rc}")
    # Subscribe to all control topics and device availability
    client.subscribe(f"{MQTT_TOPIC_CONTROL_ROOT}/#")
    client.subscribe(MQTT_TOPIC_DEVICE_AVAILABLE)

def on_message(client, userdata, msg):
    """
    Handles incoming MQTT commands.
    Supported Topics:
      - serial/control/refresh  (Payload: any)
      - serial/control/switch   (Payload: {"output": 1, "input": 2})
      - serial/control/edid     (Payload: {"input": 1, "index": 3})
      - serial/device/available (Payload: any) - triggers immediate reconnect
    """
    topic = msg.topic
    try:
        payload = msg.payload.decode("utf-8")
        data = json.loads(payload) if payload else {}
    except json.JSONDecodeError:
        data = {}

    print(f"MQTT RX: {topic} | {payload}")

    if topic == MQTT_TOPIC_DEVICE_AVAILABLE:
        # Device is available - trigger immediate reconnect
        print("Device availability signal received, triggering reconnect...")
        asyncio.run_coroutine_threadsafe(trigger_serial_reconnect(), app.loop)

    elif topic.endswith("/refresh"):
        # Trigger a full status query
        asyncio.run_coroutine_threadsafe(perform_initial_state_query(), app.loop)

    elif topic.endswith("/switch"):
        # Switch Output Source
        out_num = data.get("output")
        in_num = data.get("input")
        if out_num is not None and in_num is not None:
            cmd = f"EZS OUT{out_num} VS IN{in_num}"
            safe_serial_write(cmd)

    elif topic.endswith("/edid"):
        # Set EDID
        in_num = data.get("input")
        idx = data.get("index")
        if in_num is not None and idx is not None:
            cmd = f"EZS IN{in_num} EDID {idx}"
            safe_serial_write(cmd)

# Initialize MQTT Client
mqtt_client = mqtt.Client()
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

# --- State Parsing Logic (Same as before) ---
def update_state_from_serial(line: str):
    line = line.strip()
    if not line:
        return
        
    parts = line.split()
    if not parts:
        return

    value = parts[-1]
    line_upper = line.upper()
    
    if "CAS" in line_upper:
        device_state["cascade_mode"] = value
    elif "OUT1 VS" in line_upper:
        device_state["outputs"]["output_1_source"] = value
    elif "OUT2 VS" in line_upper:
        device_state["outputs"]["output_2_source"] = value
    elif "IN1 EDID" in line_upper:
        device_state["inputs_edid_index"]["input_1_edid_index"] = value
    elif "IN2 EDID" in line_upper:
        device_state["inputs_edid_index"]["input_2_edid_index"] = value
    elif "IN3 EDID" in line_upper:
        device_state["inputs_edid_index"]["input_3_edid_index"] = value
    elif "IN4 EDID" in line_upper:
        device_state["inputs_edid_index"]["input_4_edid_index"] = value

# --- Background Tasks ---

async def trigger_serial_reconnect():
    """Immediately attempts to reconnect to serial device."""
    print("Manual reconnect triggered...")
    if not serial_manager.is_connected():
        serial_manager.disconnect()
        if serial_manager.connect():
            device_state["device_status"] = serial_manager.get_status()
            # Perform state query after successful reconnect
            await perform_initial_state_query()
        else:
            device_state["device_status"] = serial_manager.get_status()


async def serial_reconnect_task():
    """Periodically checks connection and attempts reconnect if needed."""
    await asyncio.sleep(10)  # Initial delay to let things settle
    
    while True:
        try:
            if not serial_manager.is_connected():
                prev_status = device_state["device_status"]
                device_state["device_status"] = "Reconnecting..."
                
                print(f"Connection lost, attempting reconnect to {SERIAL_PORT}...")
                serial_manager.disconnect()
                
                if serial_manager.connect():
                    device_state["device_status"] = "Online"
                    print("✓ Reconnection successful, querying state...")
                    await perform_initial_state_query()
                else:
                    device_state["device_status"] = serial_manager.get_status()
            else:
                # Update status to reflect current connection state
                device_state["device_status"] = serial_manager.get_status()
            
        except Exception as e:
            print(f"Error in reconnect task: {e}")
        
        await asyncio.sleep(SERIAL_RECONNECT_INTERVAL)


async def serial_reader_task():
    """Reads from serial port continuously."""
    while True:
        if serial_manager.is_connected():
            try:
                # Use executor for blocking read
                line_bytes = await asyncio.to_thread(serial_manager.readline)
                line = line_bytes.decode('utf-8').strip()
                if line:
                    print(f"RX: {line}")
                    await serial_message_queue.put(line)
            except IOError:
                # Connection lost, mark as disconnected and let reconnect task handle it
                print("Serial read failed - connection lost (IOError)")
                serial_manager.disconnect()
                device_state["device_status"] = "Reconnecting..."
                await asyncio.sleep(1)
            except Exception as e:
                # Stale port or other serial errors - force disconnect and reconnect
                print(f"Serial Read Error: {e}")
                print("Forcing disconnect to trigger reconnect...")
                serial_manager.disconnect()
                device_state["device_status"] = "Reconnecting..."
                await asyncio.sleep(1)
        else:
            # Not connected, wait before checking again
            await asyncio.sleep(1)

async def status_processor_task():
    """Updates memory and pushes to MQTT."""
    while True:
        raw_message = await serial_message_queue.get()
        update_state_from_serial(raw_message)
        
        update_payload = {
            "timestamp": time.time(),
            "raw_data": raw_message,
            "current_state": device_state
        }
        
        try:
            mqtt_client.publish(MQTT_TOPIC_STATUS, json.dumps(update_payload))
        except Exception as e:
            print(f"MQTT Publish Error: {e}")

async def perform_initial_state_query():
    """Sends queries to populate the memory."""
    if not serial_manager.is_connected():
        print("Cannot query state: not connected")
        return
    
    print("Performing state query...")
    # Note: Device sends status automatically on boot/reconnect
    # EZG STA causes CMD ERR on some models, removed
    queries = ["EZG CAS"]
    
    for cmd in queries:
        safe_serial_write(cmd)
        await asyncio.sleep(0.1) 

# --- Lifecycle Events ---

@app.on_event("startup")
async def startup_event():
    # Store the loop for thread-safe calling from MQTT thread
    app.loop = asyncio.get_running_loop()
    
    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
        mqtt_client.loop_start()
        print(f"✓ MQTT connected to {MQTT_BROKER}:{MQTT_PORT}")
    except Exception as e:
        print(f"MQTT Connection Failed: {e}")

    # Start background tasks
    asyncio.create_task(serial_reader_task())
    asyncio.create_task(status_processor_task())
    asyncio.create_task(serial_reconnect_task())
    
    # Initial state query if connected
    if serial_manager.is_connected():
        asyncio.create_task(perform_initial_state_query())

@app.on_event("shutdown")
async def shutdown_event():
    mqtt_client.loop_stop()
    mqtt_client.disconnect()

# --- REST Endpoints (Using safe_serial_write) ---

@app.get("/status")
def get_system_status() -> Dict[str, Any]:
    return device_state

class EdidUpdateRequest(BaseModel):
    input_number: int = Field(..., ge=0, le=4)
    edid_index: int = Field(..., ge=0, le=16)

@app.post("/edid/set")
def set_input_edid(request: EdidUpdateRequest):
    command = f"EZS IN{request.input_number} EDID {request.edid_index}"
    safe_serial_write(command)
    return {"status": "command_sent", "command": command}

class VideoSwitchRequest(BaseModel):
    output_number: int = Field(..., ge=0, le=2)
    input_number: int = Field(..., ge=1, le=4)

@app.post("/output/switch")
def switch_output_source(request: VideoSwitchRequest):
    command = f"EZS OUT{request.output_number} VS IN{request.input_number}"
    safe_serial_write(command)
    return {"status": "command_sent", "command": command}
