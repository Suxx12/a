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

# Descargar Piggybank (con reintento y verificación)
RUN wget --no-check-certificate --tries=3 https://repo1.maven.org/maven2/org/apache/pig/piggybank/0.17.0/piggybank-0.17.0.jar -O /opt/pig/lib/piggybank.jar || \
    (echo "Failed to download piggybank.jar" && exit 1)

# Descargar dependencias Jackson (con reintento)
RUN wget --no-check-certificate --tries=3 https://repo1.maven.org/maven2/org/codehaus/jackson/jackson-core-asl/1.9.13/jackson-core-asl-1.9.13.jar -O /opt/pig/lib/jackson-core-asl-1.9.13.jar || \
    (echo "Failed to download jackson-core-asl.jar" && exit 1)
RUN wget --no-check-certificate --tries=3 https://repo1.maven.org/maven2/org/codehaus/jackson/jackson-mapper-asl/1.9.13/jackson-mapper-asl-1.9.13.jar -O /opt/pig/lib/jackson-mapper-asl-1.9.13.jar || \
    (echo "Failed to download jackson-mapper-asl.jar" && exit 1)

# Descargar Elephant Bird (con reintento) - Usar versión RC2 para elephant-bird-pig
RUN wget --no-check-certificate --tries=3 https://repo1.maven.org/maven2/com/twitter/elephantbird/elephant-bird-core/4.14/elephant-bird-core-4.14.jar -O /opt/pig/lib/elephant-bird-core-4.14.jar || \
    (echo "Failed to download elephant-bird-core.jar" && exit 1)
RUN wget --no-check-certificate --tries=3 https://repo1.maven.org/maven2/com/twitter/elephantbird/elephant-bird-pig/4.14-RC2/elephant-bird-pig-4.14-RC2.jar -O /opt/pig/lib/elephant-bird-pig-4.14.jar || \
    (echo "Failed to download elephant-bird-pig.jar" && exit 1)

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