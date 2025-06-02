#!/bin/bash
# filepath: /home/camilo/udp/sist-distribuidos/sistemas_distribuidos/tarea_2/m8-processing/run.sh

echo "Iniciando procesamiento de datos..."

# Verificar que los archivos existen
echo "Verificando archivos de entrada..."
ls -la /app/input/alertas_completas/
ls -la /app/input/atascos_completos/

# Ejecutar el script de Apache Pig para procesar los datos
echo "Ejecutando script de Pig..."
pig -x local procesar_data.pig

echo "Resultados del procesamiento:"
ls -la /app/results/

echo "Procesamiento completado."