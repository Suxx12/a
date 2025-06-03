#!/bin/bash
# filepath: /home/renato/proyectos_universidad/sistemas_distribuidos/tarea_2/m7-filtering/run.sh

echo "=== Iniciando proceso de análisis de datos de tráfico ==="
echo ""

# Paso 1: Exportar datos de MongoDB
echo "Paso 1: Exportando datos de MongoDB..."
echo ""
python3 exportar_mongo.py
echo ""

# Paso 2: Transformar datos JSON al formato para Pig
#echo "Paso 2: Transformando datos JSON para Pig..."
#echo ""
#python3 transformar_json.py /app/data /app/data_preprocesada
#echo ""

# Paso 3: Procesar datos con Apache Pig
echo "Paso 3: Procesando datos con Apache Pig..."
echo ""
pig -x local filtrar_data.pig
echo ""

echo "Proceso completado."
