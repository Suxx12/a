#!/bin/bash
# filepath: /home/renato/proyectos_universidad/sistemas_distribuidos/tarea_2/m7-filtering/run.sh

set -e

echo "=== Iniciando proceso de análisis de datos de tráfico ==="

# Paso 1: Exportar datos de MongoDB
echo "Paso 1: Exportando datos de MongoDB..."
python3 exportar_mongo.py

# Paso 2: Procesar con Pig
echo "Paso 2: Procesando datos con Apache Pig..."
pig -x local filtrar_data.pig

# Paso 3: Verificar resultados
echo "Paso 3: Verificando resultados..."
if [ -d "/app/results/total_counts" ]; then
    echo "Resultados generados correctamente:"
    
    echo "Conteos totales:"
    cat /app/results/total_counts/part-* 2>/dev/null || echo "No se encontraron resultados"
else
    echo "Error: No se generaron los resultados esperados"
    exit 1
fi

echo "=== Proceso completado exitosamente ==="

# Mantener el contenedor corriendo para inspección
echo "Manteniendo el contenedor activo para inspección..."
tail -f /dev/null