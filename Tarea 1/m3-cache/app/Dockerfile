FROM python:3.10

WORKDIR /app

COPY redis_script.py .

RUN pip install redis

CMD ["python", "redis_script.py"]
