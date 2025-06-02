#!/bin/bash
# filepath: /home/renato/proyectos_universidad/sistemas_distribuidos/tarea_2/m8-processing/run.sh

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

# Generar gráficos a partir de los resultados
echo "Generando gráficos de los resultados..."
python3 graficar_resultados.py

echo "Procesamiento completado."