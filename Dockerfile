FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY websocketservice.py ./

EXPOSE 8000

CMD ["uvicorn", "websocketservice:app", "--host", "0.0.0.0", "--port", "8000"]
