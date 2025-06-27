FROM python:3.9-slim

# Establecer el directorio de trabajo
WORKDIR /app

# Copiar dependencias y script
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY cargar_data_elastic.py .

# Ejecutar el script
CMD ["python", "cargar_data_elastic.py"]