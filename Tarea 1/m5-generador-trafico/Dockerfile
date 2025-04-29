FROM python:3.9-slim

WORKDIR /app

COPY generador_trafico.py .
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 5050

CMD ["python", "generador_trafico.py"]