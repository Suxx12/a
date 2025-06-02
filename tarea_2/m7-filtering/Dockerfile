FROM openjdk:8-jdk-slim

WORKDIR /app

# Instalar Python y dependencias necesarias
RUN apt-get update && \
    apt-get install -y python3 python3-pip wget dos2unix ca-certificates && \
    apt-get clean

# Copiar requirements e instalar dependencias de Python
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Descargar e instalar Apache Pig
RUN wget https://dlcdn.apache.org/pig/pig-0.17.0/pig-0.17.0.tar.gz && \
    tar -xzf pig-0.17.0.tar.gz -C /opt && \
    rm pig-0.17.0.tar.gz && \
    ln -s /opt/pig-0.17.0 /opt/pig

# Crear directorio para bibliotecas
RUN mkdir -p /opt/pig/lib

# Descargar Piggybank 
RUN wget https://repo1.maven.org/maven2/org/apache/pig/piggybank/0.17.0/piggybank-0.17.0.jar -O /opt/pig/lib/piggybank.jar

# Configurar permisos
RUN chmod 644 /opt/pig/lib/*.jar

# Configurar variables de entorno para Pig
ENV PIG_HOME=/opt/pig
ENV PATH=$PATH:$PIG_HOME/bin
ENV JAVA_HOME=/usr/local/openjdk-8
ENV HADOOP_HOME=/opt/pig

# Copiar scripts
COPY exportar_mongo.py .
COPY filtrar_data.pig .
COPY run.sh .
# Copiar el script de transformación de JSON
COPY transformar_json.py .
RUN chmod +x transformar_json.py

# Crear directorio para datos transformados
RUN mkdir -p /app/data_preprocesada

# Crear directorio para datos exportados y resultados
RUN mkdir -p /app/data
RUN mkdir -p /app/results

# Arreglar posibles problemas con los saltos de línea y permisos
RUN dos2unix run.sh && \
    chmod +x run.sh

# Comando por defecto
CMD ["./run.sh"]