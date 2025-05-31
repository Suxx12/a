-- filepath: /home/renato/proyectos_universidad/sistemas_distribuidos/tarea_2/m7-filtering/filtrar_data.pig

-- Cargar datos JSON como texto simple
alertas_raw = LOAD '/app/data/alertas_*.json' USING TextLoader();

-- Contar el número total de líneas (cada línea es un documento JSON)
alertas_count = GROUP alertas_raw ALL;
alertas_total = FOREACH alertas_count GENERATE 
    'Alertas' AS tipo,
    COUNT(alertas_raw) AS cantidad;

-- Cargar datos JSON de atascos como texto
atascos_raw = LOAD '/app/data/atascos_*.json' USING TextLoader();

-- Contar el número total de líneas
atascos_count = GROUP atascos_raw ALL;
atascos_total = FOREACH atascos_count GENERATE 
    'Atascos' AS tipo,
    COUNT(atascos_raw) AS cantidad;

-- Unir los resultados
resultados = UNION alertas_total, atascos_total;

-- Guardar resultados
STORE resultados INTO '/app/results/total_counts' USING PigStorage(',');

-- Mostrar los resultados en la consola
DUMP resultados;