-- Registrar bibliotecas necesarias
REGISTER '/opt/pig/lib/piggybank.jar';

-- ============================================================
-- SECCIÓN 1: PROCESAMIENTO DE ALERTAS
-- ============================================================

-- Cargar los datos de alertas desde CSV (esquema actualizado sin reportedBy ni subtype)
alertas_raw = LOAD '/app/data_preprocesada/transformed_alerta_*.csv' USING org.apache.pig.piggybank.storage.CSVLoader() AS (
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

-- ============================================================
-- SECCIÓN 2: PROCESAMIENTO DE ATASCOS
-- ============================================================

-- Cargar los datos de atascos desde CSV
atascos_raw = LOAD '/app/data_preprocesada/transformed_atasco_*.csv' USING org.apache.pig.piggybank.storage.CSVLoader() AS (
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

-- ============================================================
-- SECCIÓN 3: CREACIÓN DE DIRECTORIOS PARA RESULTADOS
-- ============================================================

-- Crear directorio para resultados
sh mkdir -p /app/results/alertas_completas;
sh mkdir -p /app/results/atascos_completos;
sh chmod -R 777 /app/results;

-- ============================================================
-- SECCIÓN 4: ALMACENAMIENTO DE RESULTADOS EN FORMATO CSV
-- ============================================================

-- Almacenar todas las alertas completas en formato CSV
STORE alertas_completas INTO '/app/results/alertas_completas/alertas_filtradas' USING PigStorage(',');

-- Almacenar todos los atascos completos en formato CSV
STORE atascos_completos INTO '/app/results/atascos_completos/atascos_filtrados' USING PigStorage(',');

-- ============================================================
-- SECCIÓN 5: ESCRITURA DE ENCABEZADOS EN ARCHIVOS CSV
-- ============================================================

-- Crear encabezados para el archivo de alertas (sin reportedBy ni subtype)
sh echo "uuid,city,municipalityUser,type,street,confidence,location_x,location_y,fecha" > /app/results/alertas_completas/encabezado.csv;
sh cat /app/results/alertas_completas/encabezado.csv /app/results/alertas_completas/alertas_filtradas/part-* > /app/results/alertas_completas/alertas_completas.csv;

-- Crear encabezados para el archivo de atascos
sh echo "uuid,severity,country,length,endnode,roadtype,speed,street,fecha,region,city" > /app/results/atascos_completos/encabezado.csv;
sh cat /app/results/atascos_completos/encabezado.csv /app/results/atascos_completos/atascos_filtrados/part-* > /app/results/atascos_completos/atascos_completos.csv;