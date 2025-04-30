FROM python:3.9-slim

WORKDIR /app

# Instalar dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar scripts
COPY visualizador_distribucion.py .
COPY . .

# Configurar para que matplotlib funcione sin interfaz gráfica
ENV PYTHONUNBUFFERED=1
ENV MPLBACKEND=Agg

CMD ["python", "visualizador_distribucion.py"]