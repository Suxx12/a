-- Registrar bibliotecas necesarias
REGISTER '/opt/pig/lib/piggybank.jar';
REGISTER '/opt/pig/lib/jackson-core-asl-1.9.13.jar';
REGISTER '/opt/pig/lib/jackson-mapper-asl-1.9.13.jar';

-- ============================================================
-- SECCIÓN 1: PROCESAMIENTO DE ALERTAS
-- ============================================================

-- Cargar los datos de alertas
alertas_raw = LOAD '/app/data_preprocesada/transformed_alerta_*.jsonl' USING PigStorage('\n') AS (json_line:chararray);

-- Parsear cada línea JSON de alertas a una tupla con expresiones regulares más robustas
alertas_parsed = FOREACH alertas_raw GENERATE 
    REGEX_EXTRACT(json_line, '"uuid"\\s*:\\s*"?([^",}]*)"?', 1) AS uuid,
    REGEX_EXTRACT(json_line, '"nThumbsUp"\\s*:\\s*(-?\\d+|null)', 1) AS nThumbsUp,
    REGEX_EXTRACT(json_line, '"city"\\s*:\\s*"([^"]*)"', 1) AS city,
    REGEX_EXTRACT(json_line, '"municipalityUser"\\s*:\\s*"([^"]*)"', 1) AS municipalityUser,
    REGEX_EXTRACT(json_line, '"type"\\s*:\\s*"([^"]*)"', 1) AS type,
    REGEX_EXTRACT(json_line, '"subtype"\\s*:\\s*"([^"]*)"', 1) AS subtype,
    REGEX_EXTRACT(json_line, '"street"\\s*:\\s*"([^"]*)"', 1) AS street,
    REGEX_EXTRACT(json_line, '"reportedBy"\\s*:\\s*"([^"]*)"', 1) AS reportedBy,
    REGEX_EXTRACT(json_line, '"confidence"\\s*:\\s*(-?[\\d\\.]+|null)', 1) AS confidence,
    REGEX_EXTRACT(json_line, '"location_x"\\s*:\\s*(-?[\\d\\.]+|null)', 1) AS location_x,
    REGEX_EXTRACT(json_line, '"location_y"\\s*:\\s*(-?[\\d\\.]+|null)', 1) AS location_y,
    REGEX_EXTRACT(json_line, '"fecha"\\s*:\\s*"([^"]*)"', 1) AS fecha;

-- Verificar que estamos cargando alertas correctamente y mostrar muestras para depuración
DESCRIBE alertas_parsed;
alertas_sample = LIMIT alertas_parsed 5;
DUMP alertas_sample;

-- Filtrar alertas con campos mínimos necesarios (menos restrictivo)
alertas_completas = FILTER alertas_parsed BY 
    (uuid IS NOT NULL AND uuid != '') AND
    (city IS NOT NULL AND city != '') AND
    (street IS NOT NULL AND street != '');

-- Contar alertas completas
alertas_completas_count = GROUP alertas_completas ALL;
total_alertas_completas = FOREACH alertas_completas_count GENERATE COUNT(alertas_completas) AS cantidad;

-- Mostrar cantidad de alertas con los campos mínimos necesarios
DUMP total_alertas_completas;

-- ============================================================
-- SECCIÓN 2: PROCESAMIENTO DE ATASCOS
-- ============================================================

-- Cargar los datos de atascos
atascos_raw = LOAD '/app/data_preprocesada/transformed_atasco_*.jsonl' USING PigStorage('\n') AS (json_line:chararray);

-- Parsear cada línea JSON de atascos a una tupla con expresiones regulares más robustas
atascos_parsed = FOREACH atascos_raw GENERATE 
    REGEX_EXTRACT(json_line, '"uuid"\\s*:\\s*"?([^",}]*)"?', 1) AS uuid,
    REGEX_EXTRACT(json_line, '"severity"\\s*:\\s*(-?[\\d\\.]+|null)', 1) AS severity,
    REGEX_EXTRACT(json_line, '"country"\\s*:\\s*"([^"]*)"', 1) AS country,
    REGEX_EXTRACT(json_line, '"length"\\s*:\\s*(-?[\\d\\.]+|null)', 1) AS length,
    REGEX_EXTRACT(json_line, '"endnode"\\s*:\\s*"([^"]*)"', 1) AS endnode,
    REGEX_EXTRACT(json_line, '"roadtype"\\s*:\\s*(-?[\\d\\.]+|null)', 1) AS roadtype,
    REGEX_EXTRACT(json_line, '"speed"\\s*:\\s*(-?[\\d\\.]+|null)', 1) AS speed,
    REGEX_EXTRACT(json_line, '"street"\\s*:\\s*"([^"]*)"', 1) AS street,
    REGEX_EXTRACT(json_line, '"fecha"\\s*:\\s*"([^"]*)"', 1) AS fecha,
    REGEX_EXTRACT(json_line, '"region"\\s*:\\s*"([^"]*)"', 1) AS region,
    REGEX_EXTRACT(json_line, '"city"\\s*:\\s*"([^"]*)"', 1) AS city;

-- Verificar que estamos cargando atascos correctamente y mostrar muestras para depuración
DESCRIBE atascos_parsed;
atascos_sample = LIMIT atascos_parsed 5;
DUMP atascos_sample;

-- Filtrar atascos con campos mínimos necesarios (menos restrictivo)
atascos_completos = FILTER atascos_parsed BY 
    (uuid IS NOT NULL AND uuid != '') AND
    (city IS NOT NULL AND city != '');

-- Contar atascos completos
atascos_completos_count = GROUP atascos_completos ALL;
total_atascos_completos = FOREACH atascos_completos_count GENERATE COUNT(atascos_completos) AS cantidad;

-- Mostrar cantidad de atascos con los campos mínimos necesarios
DUMP total_atascos_completos;

-- ============================================================
-- SECCIÓN 3: ANÁLISIS DE ALERTAS POR CIUDAD
-- ============================================================

-- Agrupar alertas por ciudad
alertas_por_ciudad = GROUP alertas_completas BY city;

-- Generar un conteo de alertas por cada ciudad
conteo_alertas_ciudad = FOREACH alertas_por_ciudad GENERATE 
    group AS ciudad, 
    COUNT(alertas_completas) AS cantidad_alertas;

-- Ordenar por cantidad de alertas (mayor a menor)
conteo_alertas_ordenado = ORDER conteo_alertas_ciudad BY cantidad_alertas DESC;

-- Mostrar conteo de alertas por ciudad
DUMP conteo_alertas_ordenado;

-- ============================================================
-- SECCIÓN 4: ANÁLISIS DE ATASCOS POR CIUDAD
-- ============================================================

-- Agrupar atascos por ciudad
atascos_por_ciudad = GROUP atascos_completos BY city;

-- Generar un conteo de atascos por cada ciudad
conteo_atascos_ciudad = FOREACH atascos_por_ciudad GENERATE 
    group AS ciudad, 
    COUNT(atascos_completos) AS cantidad_atascos;

-- Ordenar por cantidad de atascos (mayor a menor)
conteo_atascos_ordenado = ORDER conteo_atascos_ciudad BY cantidad_atascos DESC;

-- Mostrar conteo de atascos por ciudad
DUMP conteo_atascos_ordenado;

-- ============================================================
-- SECCIÓN 5: PREPARACIÓN DE DATOS PARA EXPORTACIÓN (VERSIÓN SIMPLIFICADA)
-- ============================================================

-- Pasar directamente los datos sin transformación de valores nulos
alertas_para_export = FOREACH alertas_completas GENERATE
    uuid,
    nThumbsUp,
    city,
    municipalityUser,
    type,
    subtype,
    street,
    reportedBy,
    confidence,
    location_x,
    location_y,
    fecha;

-- Pasar directamente los datos sin transformación de valores nulos
atascos_para_export = FOREACH atascos_completos GENERATE
    uuid,
    severity,
    country,
    length,
    endnode,
    roadtype,
    speed,
    street,
    fecha,
    region,
    city;
-- ============================================================
-- SECCIÓN 6: SEPARACIÓN DE ALERTAS POR CIUDAD
-- ============================================================

-- Separar alertas por ciudad
SPLIT alertas_para_export INTO 
    alertas_pudahuel IF city == 'Pudahuel',
    alertas_lampa IF city == 'Lampa',
    alertas_renca IF city == 'Renca',
    alertas_valle_grande IF city == 'Valle Grande',
    alertas_otras_ciudades OTHERWISE;

-- Crear JSON para alertas de Pudahuel
alertas_pudahuel_json = FOREACH alertas_pudahuel GENERATE
    CONCAT('{',
        CONCAT('"uuid":"', uuid, '",'),
        CONCAT('"nThumbsUp":', nThumbsUp, ','),
        CONCAT('"city":"', city, '",'),
        CONCAT('"municipalityUser":"', municipalityUser, '",'),
        CONCAT('"type":"', type, '",'),
        CONCAT('"subtype":"', subtype, '",'),
        CONCAT('"street":"', street, '",'),
        CONCAT('"reportedBy":"', reportedBy, '",'),
        CONCAT('"confidence":', confidence, ','),
        CONCAT('"location_x":', location_x, ','),
        CONCAT('"location_y":', location_y, ','),
        CONCAT('"fecha":"', fecha, '"'),
    '}');

-- Crear JSON para alertas de Lampa
alertas_lampa_json = FOREACH alertas_lampa GENERATE
    CONCAT('{',
        CONCAT('"uuid":"', uuid, '",'),
        CONCAT('"nThumbsUp":', nThumbsUp, ','),
        CONCAT('"city":"', city, '",'),
        CONCAT('"municipalityUser":"', municipalityUser, '",'),
        CONCAT('"type":"', type, '",'),
        CONCAT('"subtype":"', subtype, '",'),
        CONCAT('"street":"', street, '",'),
        CONCAT('"reportedBy":"', reportedBy, '",'),
        CONCAT('"confidence":', confidence, ','),
        CONCAT('"location_x":', location_x, ','),
        CONCAT('"location_y":', location_y, ','),
        CONCAT('"fecha":"', fecha, '"'),
    '}');

-- Crear JSON para alertas de Renca
alertas_renca_json = FOREACH alertas_renca GENERATE
    CONCAT('{',
        CONCAT('"uuid":"', uuid, '",'),
        CONCAT('"nThumbsUp":', nThumbsUp, ','),
        CONCAT('"city":"', city, '",'),
        CONCAT('"municipalityUser":"', municipalityUser, '",'),
        CONCAT('"type":"', type, '",'),
        CONCAT('"subtype":"', subtype, '",'),
        CONCAT('"street":"', street, '",'),
        CONCAT('"reportedBy":"', reportedBy, '",'),
        CONCAT('"confidence":', confidence, ','),
        CONCAT('"location_x":', location_x, ','),
        CONCAT('"location_y":', location_y, ','),
        CONCAT('"fecha":"', fecha, '"'),
    '}');

-- Crear JSON para alertas de Valle Grande
alertas_valle_grande_json = FOREACH alertas_valle_grande GENERATE
    CONCAT('{',
        CONCAT('"uuid":"', uuid, '",'),
        CONCAT('"nThumbsUp":', nThumbsUp, ','),
        CONCAT('"city":"', city, '",'),
        CONCAT('"municipalityUser":"', municipalityUser, '",'),
        CONCAT('"type":"', type, '",'),
        CONCAT('"subtype":"', subtype, '",'),
        CONCAT('"street":"', street, '",'),
        CONCAT('"reportedBy":"', reportedBy, '",'),
        CONCAT('"confidence":', confidence, ','),
        CONCAT('"location_x":', location_x, ','),
        CONCAT('"location_y":', location_y, ','),
        CONCAT('"fecha":"', fecha, '"'),
    '}');

-- Crear JSON para alertas de otras ciudades
alertas_otras_ciudades_json = FOREACH alertas_otras_ciudades GENERATE
    CONCAT('{',
        CONCAT('"uuid":"', uuid, '",'),
        CONCAT('"nThumbsUp":', nThumbsUp, ','),
        CONCAT('"city":"', city, '",'),
        CONCAT('"municipalityUser":"', municipalityUser, '",'),
        CONCAT('"type":"', type, '",'),
        CONCAT('"subtype":"', subtype, '",'),
        CONCAT('"street":"', street, '",'),
        CONCAT('"reportedBy":"', reportedBy, '",'),
        CONCAT('"confidence":', confidence, ','),
        CONCAT('"location_x":', location_x, ','),
        CONCAT('"location_y":', location_y, ','),
        CONCAT('"fecha":"', fecha, '"'),
    '}');

-- ============================================================
-- SECCIÓN 7: SEPARACIÓN DE ATASCOS POR CIUDAD
-- ============================================================

-- Separar atascos por ciudad
SPLIT atascos_para_export INTO 
    atascos_pudahuel IF city == 'Pudahuel',
    atascos_lampa IF city == 'Lampa',
    atascos_renca IF city == 'Renca',
    atascos_otras_ciudades OTHERWISE;

-- Crear JSON para atascos de Pudahuel
atascos_pudahuel_json = FOREACH atascos_pudahuel GENERATE
    CONCAT('{',
        CONCAT('"uuid":"', uuid, '",'),
        CONCAT('"severity":', severity, ','),
        CONCAT('"country":"', country, '",'),
        CONCAT('"length":', length, ','),
        CONCAT('"endnode":"', endnode, '",'),
        CONCAT('"roadtype":', roadtype, ','),
        CONCAT('"speed":', speed, ','),
        CONCAT('"street":"', street, '",'),
        CONCAT('"fecha":"', fecha, '",'),
        CONCAT('"region":"', region, '",'),
        CONCAT('"city":"', city, '"'),
    '}');

-- Crear JSON para atascos de Lampa
atascos_lampa_json = FOREACH atascos_lampa GENERATE
    CONCAT('{',
        CONCAT('"uuid":"', uuid, '",'),
        CONCAT('"severity":', severity, ','),
        CONCAT('"country":"', country, '",'),
        CONCAT('"length":', length, ','),
        CONCAT('"endnode":"', endnode, '",'),
        CONCAT('"roadtype":', roadtype, ','),
        CONCAT('"speed":', speed, ','),
        CONCAT('"street":"', street, '",'),
        CONCAT('"fecha":"', fecha, '",'),
        CONCAT('"region":"', region, '",'),
        CONCAT('"city":"', city, '"'),
    '}');

-- Crear JSON para atascos de Renca
atascos_renca_json = FOREACH atascos_renca GENERATE
    CONCAT('{',
        CONCAT('"uuid":"', uuid, '",'),
        CONCAT('"severity":', severity, ','),
        CONCAT('"country":"', country, '",'),
        CONCAT('"length":', length, ','),
        CONCAT('"endnode":"', endnode, '",'),
        CONCAT('"roadtype":', roadtype, ','),
        CONCAT('"speed":', speed, ','),
        CONCAT('"street":"', street, '",'),
        CONCAT('"fecha":"', fecha, '",'),
        CONCAT('"region":"', region, '",'),
        CONCAT('"city":"', city, '"'),
    '}');

-- Crear JSON para atascos de otras ciudades
atascos_otras_ciudades_json = FOREACH atascos_otras_ciudades GENERATE
    CONCAT('{',
        CONCAT('"uuid":"', uuid, '",'),
        CONCAT('"severity":', severity, ','),
        CONCAT('"country":"', country, '",'),
        CONCAT('"length":', length, ','),
        CONCAT('"endnode":"', endnode, '",'),
        CONCAT('"roadtype":', roadtype, ','),
        CONCAT('"speed":', speed, ','),
        CONCAT('"street":"', street, '",'),
        CONCAT('"fecha":"', fecha, '",'),
        CONCAT('"region":"', region, '",'),
        CONCAT('"city":"', city, '"'),
    '}');

-- ============================================================
-- SECCIÓN 8: CREACIÓN DE DATASETS COMBINADOS EN FORMATO JSON
-- ============================================================

-- Crear JSON para todas las alertas
alertas_todas_json = FOREACH alertas_para_export GENERATE
    CONCAT('{',
        CONCAT('"uuid":"', uuid, '",'),
        CONCAT('"nThumbsUp":', nThumbsUp, ','),
        CONCAT('"city":"', city, '",'),
        CONCAT('"municipalityUser":"', municipalityUser, '",'),
        CONCAT('"type":"', type, '",'),
        CONCAT('"subtype":"', subtype, '",'),
        CONCAT('"street":"', street, '",'),
        CONCAT('"reportedBy":"', reportedBy, '",'),
        CONCAT('"confidence":', confidence, ','),
        CONCAT('"location_x":', location_x, ','),
        CONCAT('"location_y":', location_y, ','),
        CONCAT('"fecha":"', fecha, '"'),
    '}');

-- Crear JSON para todos los atascos
atascos_todos_json = FOREACH atascos_para_export GENERATE
    CONCAT('{',
        CONCAT('"uuid":"', uuid, '",'),
        CONCAT('"severity":', severity, ','),
        CONCAT('"country":"', country, '",'),
        CONCAT('"length":', length, ','),
        CONCAT('"endnode":"', endnode, '",'),
        CONCAT('"roadtype":', roadtype, ','),
        CONCAT('"speed":', speed, ','),
        CONCAT('"street":"', street, '",'),
        CONCAT('"fecha":"', fecha, '",'),
        CONCAT('"region":"', region, '",'),
        CONCAT('"city":"', city, '"'),
    '}');

-- ============================================================
-- SECCIÓN 9: VERIFICACIÓN DE CONJUNTOS NO VACÍOS
-- ============================================================

-- Verificar conjuntos de alertas antes de almacenar
alertas_todas_count = GROUP alertas_todas_json ALL;
DUMP alertas_todas_count;

-- Verificar conjuntos de atascos antes de almacenar
atascos_todos_count = GROUP atascos_todos_json ALL;
DUMP atascos_todos_count;

-- ============================================================
-- SECCIÓN 10: ALMACENAMIENTO DE RESULTADOS
-- ============================================================

-- Almacenar todas las alertas en formato JSON
STORE alertas_todas_json INTO '/app/results/alertas/todas_alertas_json' USING PigStorage('\n');

-- Almacenar todos los atascos en formato JSON
STORE atascos_todos_json INTO '/app/results/atascos/todos_atascos_json' USING PigStorage('\n');

-- Almacenar alertas JSON por ciudad (sólo si hay datos)
STORE alertas_pudahuel_json INTO '/app/results/por_ciudad/alertas/pudahuel_json' USING PigStorage('\n');
STORE alertas_lampa_json INTO '/app/results/por_ciudad/alertas/lampa_json' USING PigStorage('\n');
STORE alertas_renca_json INTO '/app/results/por_ciudad/alertas/renca_json' USING PigStorage('\n');
STORE alertas_valle_grande_json INTO '/app/results/por_ciudad/alertas/valle_grande_json' USING PigStorage('\n');
STORE alertas_otras_ciudades_json INTO '/app/results/por_ciudad/alertas/otras_ciudades_json' USING PigStorage('\n');

-- Almacenar atascos JSON por ciudad (sólo si hay datos)
STORE atascos_pudahuel_json INTO '/app/results/por_ciudad/atascos/pudahuel_json' USING PigStorage('\n');
STORE atascos_lampa_json INTO '/app/results/por_ciudad/atascos/lampa_json' USING PigStorage('\n');
STORE atascos_renca_json INTO '/app/results/por_ciudad/atascos/renca_json' USING PigStorage('\n');
STORE atascos_otras_ciudades_json INTO '/app/results/por_ciudad/atascos/otras_ciudades_json' USING PigStorage('\n');

-- También almacenar todos los datos en formato tabular para compatibilidad
STORE alertas_para_export INTO '/app/results/alertas/todos_alertas' USING PigStorage('\t');
STORE atascos_para_export INTO '/app/results/atascos/todos_atascos' USING PigStorage('\t');