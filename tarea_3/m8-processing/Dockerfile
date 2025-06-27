FROM openjdk:8-jdk-slim

WORKDIR /app

# Instalar wget, Python3, pip y otras herramientas necesarias
RUN apt-get update && \
    apt-get install -y wget dos2unix python3 python3-pip python3-dev build-essential \
                       libfreetype6-dev pkg-config libpng-dev && \
    apt-get clean

# Descargar e instalar Apache Pig
RUN wget https://dlcdn.apache.org/pig/pig-0.17.0/pig-0.17.0.tar.gz && \
    tar -xzf pig-0.17.0.tar.gz -C /opt && \
    rm pig-0.17.0.tar.gz && \
    ln -s /opt/pig-0.17.0 /opt/pig

# Crear directorio para bibliotecas
RUN mkdir -p /opt/pig/lib

# Descargar Piggybank (simple, sin reintentos ni verificación)
RUN wget https://repo1.maven.org/maven2/org/apache/pig/piggybank/0.17.0/piggybank-0.17.0.jar -O /opt/pig/lib/piggybank.jar

# Configurar permisos
RUN chmod 644 /opt/pig/lib/*.jar

# Configurar variables de entorno para Pig
ENV PIG_HOME=/opt/pig
ENV PATH=$PATH:$PIG_HOME/bin
ENV JAVA_HOME=/usr/local/openjdk-8
ENV HADOOP_HOME=/opt/pig

# Copiar scripts
COPY procesar_data.pig .
COPY run.sh .
COPY graficar_resultados.py .

# Opción 1: Si usas un archivo requirements.txt
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt


# Crear directorio para datos importados de m7 y resultados
RUN mkdir -p /app/input 
RUN mkdir -p /app/results

# Arreglar posibles problemas con los saltos de línea y permisos
RUN dos2unix run.sh && \
    chmod +x run.sh && \
    chmod +x graficar_resultados.py

# Comando por defecto
CMD ["./run.sh"]