-- Registrar PiggyBank (para poder usar funciones adicionales)
REGISTER '/opt/pig/lib/piggybank.jar';

-- Encontrar el directorio de ejecución más reciente para entrada
%declare latest_dir `ls -td /app/input/ejecucion_* | head -1`
%declare timestamp `date +%Y%m%d_%H%M%S`
%declare output_dir '/app/results/ejecucion_$timestamp'

-- Crear directorio para la ejecución actual
sh mkdir -p $output_dir;
sh echo "Usando datos de entrada de: $latest_dir";
sh echo "Guardando resultados en: $output_dir";

-- Cargar datos de alertas
alertas = LOAD '$latest_dir/alertas_completas/alertas_completas.csv' USING PigStorage(',') AS (
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

-- Cargar datos de atascos
atascos = LOAD '$latest_dir/atascos_completos/atascos_completos.csv' USING PigStorage(',') AS (
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

-- Verificar si los datos se cargaron correctamente mostrando una muestra
alertas_sample = LIMIT alertas 10;
atascos_sample = LIMIT atascos 10;

-- Guardar resultados de muestra para verificar
STORE alertas_sample INTO '$output_dir/alertas_sample' USING PigStorage(',');
STORE atascos_sample INTO '$output_dir/atascos_sample' USING PigStorage(',');

-- Contar registros en cada conjunto de datos
alertas_count = FOREACH (GROUP alertas ALL) GENERATE COUNT(alertas) AS count;
atascos_count = FOREACH (GROUP atascos ALL) GENERATE COUNT(atascos) AS count;

-- Guardar conteos
STORE alertas_count INTO '$output_dir/alertas_count' USING PigStorage(',');
STORE atascos_count INTO '$output_dir/atascos_count' USING PigStorage(',');

-- Extraer la hora de las alertas para análisis de hora pico
alertas_con_hora = FOREACH alertas GENERATE 
    uuid,
    city,
    municipalityUser,
    type,
    street,
    confidence,
    location_x,
    location_y,
    fecha,
    SUBSTRING(fecha, 11, 13) AS hora;

-- Agrupar alertas por hora del día
alertas_por_hora = GROUP alertas_con_hora BY hora;
conteo_alertas_por_hora = FOREACH alertas_por_hora GENERATE 
    group AS hora, 
    COUNT(alertas_con_hora) AS num_alertas;
horas_ordenadas = ORDER conteo_alertas_por_hora BY num_alertas DESC;

-- Guardar horas con más alertas (horas pico)
STORE horas_ordenadas INTO '$output_dir/horas_pico_data' USING PigStorage(',');

-- Crear archivo CSV con encabezados para horas pico
sh echo "hora,cantidad_alertas" > $output_dir/encabezado_horas_pico.csv;
sh cat $output_dir/encabezado_horas_pico.csv $output_dir/horas_pico_data/part-* > $output_dir/horas_pico.csv;

-- Calcular ciudades con más alertas ordenadas de mayor a menor
alertas_por_ciudad = GROUP alertas BY city;
conteo_alertas_por_ciudad = FOREACH alertas_por_ciudad GENERATE 
    group AS city, 
    COUNT(alertas) AS num_alertas;
ciudades_ordenadas = ORDER conteo_alertas_por_ciudad BY num_alertas DESC;

-- Guardar las ciudades con más alertas en CSV
STORE ciudades_ordenadas INTO '$output_dir/ciudades_mas_alertas' USING PigStorage(',');

-- Crear archivo CSV con encabezados para ciudades
sh echo "comuna,cantidad_alertas" > $output_dir/encabezado_comunas.csv;
sh cat $output_dir/encabezado_comunas.csv $output_dir/ciudades_mas_alertas/part-* > $output_dir/comunas_con_mas_alertas.csv;

-- Calcular tipos de alerta más frecuentes
alertas_por_tipo = GROUP alertas BY type;
conteo_alertas_por_tipo = FOREACH alertas_por_tipo GENERATE 
    group AS tipo_alerta, 
    COUNT(alertas) AS num_alertas;
tipos_ordenados = ORDER conteo_alertas_por_tipo BY num_alertas DESC;

-- Guardar los tipos de alerta más frecuentes en CSV
STORE tipos_ordenados INTO '$output_dir/tipos_mas_frecuentes' USING PigStorage(',');

-- Crear archivo CSV con encabezados para tipos de alerta
sh echo "tipo_alerta,cantidad" > $output_dir/encabezado_tipos.csv;
sh cat $output_dir/encabezado_tipos.csv $output_dir/tipos_mas_frecuentes/part-* > $output_dir/tipos_alerta_frecuencia.csv;

-- Filtrar registros con type = 'false'
alertas_false = FILTER alertas BY type == 'false';

-- Guardar los registros con type = 'false' en CSV
STORE alertas_false INTO '$output_dir/alertas_false' USING PigStorage(',');

-- Crear archivo CSV con encabezados para alertas false
sh echo "uuid,city,municipalityUser,type,street,confidence,location_x,location_y,fecha" > $output_dir/encabezado_alertas_false.csv;
sh cat $output_dir/encabezado_alertas_false.csv $output_dir/alertas_false/part-* > $output_dir/alertas_tipo_false.csv;

-- Filtrar alertas de tipo ACCIDENT
alertas_accident = FILTER alertas BY type == 'ACCIDENT';

-- Calcular cantidad de accidentes por comuna
accidentes_por_comuna = GROUP alertas_accident BY city;
conteo_accidentes_por_comuna = FOREACH accidentes_por_comuna GENERATE 
    group AS comuna, 
    COUNT(alertas_accident) AS num_accidentes;
comunas_ordenadas_por_accidentes = ORDER conteo_accidentes_por_comuna BY num_accidentes DESC;

-- Guardar comunas con más accidentes en CSV
STORE comunas_ordenadas_por_accidentes INTO '$output_dir/comunas_con_mas_accidentes_data' USING PigStorage(',');

-- Crear archivo CSV con encabezados para accidentes por comuna
sh echo "comuna,cantidad_accidentes" > $output_dir/encabezado_accidentes_comuna.csv;
sh cat $output_dir/encabezado_accidentes_comuna.csv $output_dir/comunas_con_mas_accidentes_data/part-* > $output_dir/comunas_con_mas_accidentes.csv;

-- Contar total de accidentes
accidentes_count = FOREACH (GROUP alertas_accident ALL) GENERATE COUNT(alertas_accident) AS count;
STORE accidentes_count INTO '$output_dir/accidentes_count' USING PigStorage(',');


-- Calcular calles con más alertas
alertas_por_calle = GROUP alertas BY street;
conteo_alertas_por_calle = FOREACH alertas_por_calle GENERATE 
    group AS calle, 
    COUNT(alertas) AS num_alertas;
calles_ordenadas = ORDER conteo_alertas_por_calle BY num_alertas DESC;

-- Guardar las calles con más alertas en CSV
STORE calles_ordenadas INTO '$output_dir/calles_mas_alertas_data' USING PigStorage(',');

-- Crear archivo CSV con encabezados para calles
sh echo "calle,cantidad_alertas" > $output_dir/encabezado_calles.csv;
sh cat $output_dir/encabezado_calles.csv $output_dir/calles_mas_alertas_data/part-* > $output_dir/calles_con_mas_alertas.csv;

-- Calcular calles con mas accidentes
accidentes_group_by_calle_ciudad = GROUP alertas_accident BY (street, city);
conteo_accidentes_por_calle_ciudad = FOREACH accidentes_group_by_calle_ciudad GENERATE 
    group.street AS calle, 
    group.city AS ciudad,
    COUNT(alertas_accident) AS num_accidentes;
calles_ordenadas_por_accidentes = ORDER conteo_accidentes_por_calle_ciudad BY num_accidentes DESC;
-- Guardar las calles con más accidentes en CSV
STORE calles_ordenadas_por_accidentes INTO '$output_dir/calles_mas_accidentes_data' USING PigStorage(',');
-- Crear archivo CSV con encabezados para calles con accidentes
sh echo "calle,ciudad,cantidad_accidentes" > $output_dir/encabezado_calles_accidentes.csv;
sh cat $output_dir/encabezado_calles_accidentes.csv $output_dir/calles_mas_accidentes_data/part-* > $output_dir/calles_con_mas_accidentes.csv;

-- Calcular atascos mas largos
atascos_ordenados_por_longitud = ORDER atascos BY length DESC;
STORE atascos_ordenados_por_longitud INTO '$output_dir/atascos_mas_largos' USING PigStorage(',');
-- Crear archivo CSV con encabezados para atascos largos
sh echo "uuid,severity,country,length,endnode,roadtype,speed,street,fecha,region,city" > $output_dir/encabezado_atascos_largos.csv;
sh cat $output_dir/encabezado_atascos_largos.csv $output_dir/atascos_mas_largos/part-* > $output_dir/atascos_largos.csv;

atascos_por_ciudad = GROUP atascos BY city;
atascos_por_ciudad_sum = FOREACH atascos_por_ciudad GENERATE 
    group AS ciudad, 
    SUM(atascos.length) AS largo_total,
    COUNT(atascos) AS num_atascos;
atascos_ciudades_ordenados = ORDER atascos_por_ciudad_sum BY largo_total DESC;
-- Guardar atascos por ciudad en CSV
STORE atascos_ciudades_ordenados INTO '$output_dir/atascos_por_ciudad' USING PigStorage(',');
-- Crear archivo CSV con encabezados para atascos por ciudad
sh echo "ciudad,largo_total,num_atascos" > $output_dir/encabezado_atascos_ciudad.csv;
sh cat $output_dir/encabezado_atascos_ciudad.csv $output_dir/atascos_por_ciudad/part-* > $output_dir/atascos_por_ciudad.csv;

-- Mensaje para el registro
DUMP alertas_count;
DUMP atascos_count;
DUMP horas_ordenadas;