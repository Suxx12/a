FROM python:3.9-slim

WORKDIR /app

COPY load_json.py .
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "load_json.py"]