FROM python:3.9-slim

WORKDIR /app

COPY generador_trafico_api.py .
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "generador_trafico_api.py"]