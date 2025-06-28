-- Registrar bibliotecas necesarias
REGISTER '/opt/pig/lib/piggybank.jar';

-- Encontrar el directorio de ejecución más reciente
%declare CURRENT_TIMESTAMP `date +%Y%m%d_%H%M%S`
%declare LATEST_DIR `ls -td /app/data/ejecucion_* | head -1`
%declare OUTPUT_DIR '/app/results/ejecucion_$CURRENT_TIMESTAMP'

-- Crear directorio para resultados con timestamp actual
sh mkdir -p $OUTPUT_DIR/alertas_completas;
sh mkdir -p $OUTPUT_DIR/atascos_completos;
sh chmod -R 777 $OUTPUT_DIR;

-- ============================================================
-- SECCIÓN 1: PROCESAMIENTO DE ALERTAS
-- ============================================================

-- Cargar los datos de alertas desde CSV con delimitador ,
alertas_raw = LOAD '$LATEST_DIR/transformed_alerta_*.csv' USING PigStorage(',') AS (
    uuid:chararray, 
    city:chararray, 
    municipalityUser:chararray, 
    type:chararray, 
    street:chararray, 
    confidence:double, 
    location_x:double, 
    location_y:double, 
    fecha:chararray
);

-- Filtrar la fila de encabezado
alertas_sin_encabezado = FILTER alertas_raw BY uuid != 'uuid';

-- Filtrar alertas con TODOS los campos completos (sin reportedBy ni subtype)
alertas_completas = FILTER alertas_sin_encabezado BY 
    (uuid IS NOT NULL AND uuid != '') AND
    (city IS NOT NULL AND city != '') AND
    (municipalityUser IS NOT NULL AND municipalityUser != '') AND
    (type IS NOT NULL AND type != '') AND
    (street IS NOT NULL AND street != '') AND
    (confidence IS NOT NULL) AND
    (location_x IS NOT NULL) AND
    (location_y IS NOT NULL) AND
    (fecha IS NOT NULL AND fecha != '');

-- Eliminar alertas con UUIDs duplicados (conservar solo la primera aparición)
alertas_agrupadas = GROUP alertas_completas BY uuid;
alertas_sin_duplicados = FOREACH alertas_agrupadas {
    -- Ordenar por fecha y tomar solo el primer registro
    ordenadas = ORDER alertas_completas BY fecha ASC;
    primera = LIMIT ordenadas 1;
    GENERATE FLATTEN(primera);
};

--
-- SECCIÓN 2: PROCESAMIENTO DE ATASCOS
-- 

-- Cargar los datos de atascos desde CSV con delimitador ,
atascos_raw = LOAD '$LATEST_DIR/transformed_atasco_*.csv' USING PigStorage(',') AS (
    uuid:chararray, 
    severity:int, 
    country:chararray, 
    length:int, 
    endnode:chararray, 
    roadtype:int, 
    speed:double, 
    street:chararray, 
    fecha:chararray, 
    region:chararray, 
    city:chararray
);

-- Filtrar la fila de encabezado
atascos_sin_encabezado = FILTER atascos_raw BY uuid != 'uuid';

-- Filtrar atascos con TODOS los campos completos
atascos_completos = FILTER atascos_sin_encabezado BY 
    (uuid IS NOT NULL AND uuid != '') AND
    (severity IS NOT NULL) AND
    (country IS NOT NULL AND country != '') AND
    (length IS NOT NULL) AND
    (endnode IS NOT NULL AND endnode != '') AND
    (roadtype IS NOT NULL) AND
    (speed IS NOT NULL) AND
    (street IS NOT NULL AND street != '') AND
    (fecha IS NOT NULL AND fecha != '') AND
    (region IS NOT NULL AND region != '') AND
    (city IS NOT NULL AND city != '');

-- Eliminar atascos con UUIDs duplicados (conservar solo la primera aparición)
atascos_agrupados = GROUP atascos_completos BY uuid;
atascos_sin_duplicados = FOREACH atascos_agrupados {
    -- Ordenar por fecha y tomar solo el primer registro
    ordenados = ORDER atascos_completos BY fecha ASC;
    primero = LIMIT ordenados 1;
    GENERATE FLATTEN(primero);
};

-- 
-- SECCIÓN 4: ALMACENAMIENTO DE RESULTADOS EN FORMATO CSV
--

-- Almacenar todas las alertas completas sin duplicados en formato CSV con delimitador ,
STORE alertas_sin_duplicados INTO '$OUTPUT_DIR/alertas_completas/alertas_filtradas' USING PigStorage(',');

-- Almacenar todos los atascos completos sin duplicados en formato CSV con delimitador ,
STORE atascos_sin_duplicados INTO '$OUTPUT_DIR/atascos_completos/atascos_filtrados' USING PigStorage(',');

-- 
-- SECCIÓN 5: ESCRITURA DE ENCABEZADOS EN ARCHIVOS CSV
--

-- Crear encabezados para el archivo de alertas (sin reportedBy ni subtype) con delimitador ,
sh echo "uuid,city,municipalityUser,type,street,confidence,location_x,location_y,fecha" > $OUTPUT_DIR/alertas_completas/encabezado.csv;
sh cat $OUTPUT_DIR/alertas_completas/encabezado.csv $OUTPUT_DIR/alertas_completas/alertas_filtradas/part-* > $OUTPUT_DIR/alertas_completas/alertas_completas.csv;

-- Crear encabezados para el archivo de atascos con delimitador ,
sh echo "uuid,severity,country,length,endnode,roadtype,speed,street,fecha,region,city" > $OUTPUT_DIR/atascos_completos/encabezado.csv;
sh cat $OUTPUT_DIR/atascos_completos/encabezado.csv $OUTPUT_DIR/atascos_completos/atascos_filtrados/part-* > $OUTPUT_DIR/atascos_completos/atascos_completos.csv;

--
-- SECCIÓN 6: ESTADÍSTICAS DE PROCESAMIENTO
--

-- Contar registros en cada etapa
alertas_total = GROUP alertas_sin_encabezado ALL;
alertas_total_count = FOREACH alertas_total GENERATE COUNT(alertas_sin_encabezado) as count;
alertas_completas_total = GROUP alertas_completas ALL;
alertas_completas_count = FOREACH alertas_completas_total GENERATE COUNT(alertas_completas) as count;
alertas_sin_duplicados_total = GROUP alertas_sin_duplicados ALL;
alertas_sin_duplicados_count = FOREACH alertas_sin_duplicados_total GENERATE COUNT(alertas_sin_duplicados) as count;

atascos_total = GROUP atascos_sin_encabezado ALL;
atascos_total_count = FOREACH atascos_total GENERATE COUNT(atascos_sin_encabezado) as count;
atascos_completos_total = GROUP atascos_completos ALL;
atascos_completos_count = FOREACH atascos_completos_total GENERATE COUNT(atascos_completos) as count;
atascos_sin_duplicados_total = GROUP atascos_sin_duplicados ALL;
atascos_sin_duplicados_count = FOREACH atascos_sin_duplicados_total GENERATE COUNT(atascos_sin_duplicados) as count;

-- Mostrar información de directorios usados
sh echo "Usando datos de: $LATEST_DIR";
sh echo "Guardando resultados en: $OUTPUT_DIR";

DUMP alertas_total_count;
DUMP alertas_completas_count;
DUMP alertas_sin_duplicados_count;
DUMP atascos_total_count;
DUMP atascos_completos_count;
DUMP atascos_sin_duplicados_count;