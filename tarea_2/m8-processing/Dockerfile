FROM openjdk:8-jdk-slim

WORKDIR /app

# Instalar wget y otras herramientas necesarias
RUN apt-get update && \
    apt-get install -y wget dos2unix && \
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

# Crear directorio para datos importados de m7 y resultados
RUN mkdir -p /app/input 
RUN mkdir -p /app/results

# Arreglar posibles problemas con los saltos de línea y permisos
RUN dos2unix run.sh && \
    chmod +x run.sh

# Comando por defecto
CMD ["./run.sh"]