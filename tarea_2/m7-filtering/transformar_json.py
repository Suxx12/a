#!/usr/bin/env python3
import json
import os
import glob
import sys
import csv
from datetime import datetime

def procesar_alertas(archivo_entrada, archivo_salida):
    """
    Procesa un archivo JSON de alertas y extrae los campos específicos requeridos.
    Guarda el resultado en formato CSV con delimitador ','.
    """
    print(f"Procesando alertas en {archivo_entrada}...")
    
    try:
        with open(archivo_entrada, 'r') as f:
            try:
                # Cargar todo el archivo como un único objeto JSON
                datos = json.load(f)
                
                # Verificar si es un diccionario donde cada clave es un UUID
                if isinstance(datos, dict) and all(isinstance(datos.get(k), dict) for k in datos if isinstance(k, str)):
                    print(f"Formato detectado: diccionario de alertas. Procesando {len(datos)} registros...")
                    
                    # Definir los campos para el CSV (encabezados) - sin reportedBy ni subtype
                    campos = [
                        "uuid", "city", "municipalityUser", "type", 
                        "street", "confidence", 
                        "location_x", "location_y", "fecha"
                    ]
                    
                    # Preparar datos preprocesados
                    filas_procesadas = []
                    
                    # Procesar todos los registros
                    for uuid, alerta in datos.items():
                        # Obtener ciudad y reemplazar comas por punto y coma
                        city = alerta.get("city", "")
                        if city and isinstance(city, str):
                            # Reemplazar comas por punto y coma y estandarizar formato
                            city = city.replace(',', ';')
                            # Convertir a formato título (primera letra de cada palabra en mayúscula)
                            city_parts = city.split(';')
                            city_parts = [part.strip().title() for part in city_parts]
                            city = ';'.join(city_parts)
                        
                        # Crear un diccionario con los campos requeridos - sin reportedBy ni subtype
                        fila = {
                            "uuid": alerta.get("uuid", uuid),
                            "city": city,
                            "municipalityUser": alerta.get("reportByMunicipalityUser", ""),
                            "type": alerta.get("type", ""),
                            "street": alerta.get("street", ""),
                            "confidence": alerta.get("confidence", 0),
                            "location_x": alerta.get("location", {}).get("x", alerta.get("x", 0)),
                            "location_y": alerta.get("location", {}).get("y", alerta.get("y", 0)),
                            "fecha": alerta.get("fecha", "")
                        }
                        
                        filas_procesadas.append(fila)
                    
                    # Ahora escribir al CSV
                    with open(archivo_salida, 'w', newline='') as f_out:
                        # Crear escritor CSV
                        writer = csv.DictWriter(
                            f_out, 
                            fieldnames=campos, 
                            delimiter=',', 
                            quotechar='"', 
                            quoting=csv.QUOTE_MINIMAL  # Solo poner comillas cuando sea necesario
                        )
                        
                        # Escribir encabezados
                        writer.writeheader()
                        
                        # Escribir todas las filas procesadas
                        for fila in filas_procesadas:
                            writer.writerow(fila)
                    
                    return True
                else:
                    print(f"El archivo {archivo_entrada} no tiene el formato esperado de diccionario de alertas.")
                    return False
            
            except json.JSONDecodeError as e:
                print(f"Error al decodificar JSON en {archivo_entrada}: {e}")
                return False
    
    except Exception as e:
        print(f"Error al procesar archivo {archivo_entrada}: {e}")
        return False

def procesar_atascos(archivo_entrada, archivo_salida):
    """
    Procesa un archivo JSON de atascos y extrae los campos específicos requeridos.
    Guarda el resultado en formato CSV con delimitador ','.
    """
    print(f"Procesando atascos en {archivo_entrada}...")
    
    try:
        # Primero cargar el JSON
        with open(archivo_entrada, 'r') as f:
            try:
                # Cargar todo el archivo como un único objeto JSON
                datos = json.load(f)
                
                # Verificar si es un diccionario donde cada clave es un UUID
                if isinstance(datos, dict) and all(isinstance(datos.get(k), dict) for k in datos if isinstance(k, str)):
                    print(f"Formato detectado: diccionario de atascos. Procesando {len(datos)} registros...")
                    
                    # Preparar datos preprocesados con city modificado
                    filas_procesadas = []
                    
                    # Procesar todos los registros
                    for uuid, atasco in datos.items():
                        # Obtener ciudad y reemplazar comas por punto y coma
                        city = atasco.get("city", "")
                        if city and isinstance(city, str):
                            # Reemplazar comas por punto y coma y estandarizar formato
                            city = city.replace(',', ';')
                            # Convertir a formato título (primera letra de cada palabra en mayúscula)
                            city_parts = city.split(';')
                            city_parts = [part.strip().title() for part in city_parts]
                            city = ';'.join(city_parts)
                        
                        # Crear un diccionario con los campos requeridos
                        fila = {
                            "uuid": atasco.get("uuid", uuid),
                            "severity": atasco.get("severity", ""),
                            "country": atasco.get("country", ""),
                            "length": atasco.get("length", ""),
                            "endnode": atasco.get("endNode", ""),
                            "roadtype": atasco.get("roadType", ""),
                            "speed": atasco.get("speedKMH", atasco.get("speed", "")),
                            "street": atasco.get("street", ""),
                            "fecha": atasco.get("fecha", ""),
                            "region": atasco.get("region", ""),
                            "city": city
                        }
                        
                        filas_procesadas.append(fila)
                    
                    # Ahora escribir directamente al CSV con configuración controlada
                    with open(archivo_salida, 'w', newline='') as f_out:
                        # Definir los campos para el CSV (encabezados)
                        campos = [
                            "uuid", "severity", "country", "length", "endnode", "roadtype", 
                            "speed", "street", "fecha", "region", "city"
                        ]
                        
                        # Crear escritor CSV
                        writer = csv.DictWriter(
                            f_out, 
                            fieldnames=campos, 
                            delimiter=',', 
                            quotechar='"', 
                            quoting=csv.QUOTE_MINIMAL  # Solo poner comillas cuando sea necesario
                        )
                        
                        # Escribir encabezados
                        writer.writeheader()
                        
                        # Escribir todas las filas procesadas
                        for fila in filas_procesadas:
                            writer.writerow(fila)
                    
                    # Verificar resultados
                    with open(archivo_salida, 'r') as check_file:
                        contenido = check_file.read(1000)  # Leer los primeros 1000 caracteres
                        if ';' in contenido:
                            print(f"✅ Se encontró al menos un punto y coma en los datos procesados.")
                        else:
                            print(f"⚠️ No se encontraron puntos y comas en la muestra inicial.")
                    
                    return True
                else:
                    print(f"El archivo {archivo_entrada} no tiene el formato esperado de diccionario de atascos.")
                    return False
            
            except json.JSONDecodeError as e:
                print(f"Error al decodificar JSON en {archivo_entrada}: {e}")
                return False
    
    except Exception as e:
        print(f"Error al procesar archivo {archivo_entrada}: {e}")
        return False

def main():
    # Comprobar argumentos
    if len(sys.argv) < 3:
        print("Uso: transformar_json.py <directorio_entrada> <directorio_salida>")
        sys.exit(1)
    
    dir_entrada = sys.argv[1]
    dir_salida = sys.argv[2]
    
    # Verificar que los directorios existen
    if not os.path.isdir(dir_entrada):
        print(f"ERROR: El directorio de entrada '{dir_entrada}' no existe")
        sys.exit(1)
    
    # Crear directorio de salida si no existe
    os.makedirs(dir_salida, exist_ok=True)
    
    # Verificar que tengamos permisos de escritura en el directorio de salida
    if not os.access(dir_salida, os.W_OK):
        print(f"ERROR: No tienes permisos de escritura en {dir_salida}")
        try:
            print(f"Ejecutando: chmod 777 {dir_salida}")
            os.system(f"chmod 777 {dir_salida}")
        except Exception as e:
            print(f"No se pudieron cambiar los permisos: {e}")
            print("Intentando continuar de todas formas...")
    
    # Buscar archivos de alertas (empiezan con "alertas")
    archivos_alertas = glob.glob(os.path.join(dir_entrada, "alertas_*.json"))
    print(f"Encontrados {len(archivos_alertas)} archivos de ALERTAS en {dir_entrada}")
    
    # Buscar archivos de atascos (empiezan con "atascos")
    archivos_atascos = glob.glob(os.path.join(dir_entrada, "atascos_*.json"))
    print(f"Encontrados {len(archivos_atascos)} archivos de ATASCOS en {dir_entrada}")
    
    # Procesar archivos de alertas
    alertas_procesadas = 0
    for archivo in archivos_alertas:
        nombre_base = os.path.basename(archivo)
        nombre_base_sin_extension = os.path.splitext(nombre_base)[0]
        nombre_salida = os.path.join(dir_salida, f"transformed_alerta_{nombre_base_sin_extension}.csv")
        
        if procesar_alertas(archivo, nombre_salida):
            alertas_procesadas += 1
    
    # Procesar archivos de atascos
    atascos_procesados = 0
    for archivo in archivos_atascos:
        nombre_base = os.path.basename(archivo)
        nombre_base_sin_extension = os.path.splitext(nombre_base)[0]
        nombre_salida = os.path.join(dir_salida, f"transformed_atasco_{nombre_base_sin_extension}.csv")
        
        if procesar_atascos(archivo, nombre_salida):
            atascos_procesados += 1
    
    # Mostrar resumen del procesamiento
    print(f"Proceso completado.")
    print(f"- Alertas procesadas: {alertas_procesadas}/{len(archivos_alertas)}")
    print(f"- Atascos procesados: {atascos_procesados}/{len(archivos_atascos)}")
    
    # Verificar que los archivos se hayan creado correctamente
    archivos_alertas_transformed = glob.glob(os.path.join(dir_salida, "transformed_alerta_*.csv"))
    archivos_atascos_transformed = glob.glob(os.path.join(dir_salida, "transformed_atasco_*.csv"))
    
    print(f"Archivos de alertas creados: {len(archivos_alertas_transformed)}")
    print(f"Archivos de atascos creados: {len(archivos_atascos_transformed)}")
    
    # Intentar leer un archivo transformado para verificar
    if archivos_alertas_transformed:
        try:
            with open(archivos_alertas_transformed[0], 'r') as f:
                primeras_lineas = f.readlines()[:2]  # Leer encabezado y primera fila
                if primeras_lineas:
                    print(f"Encabezado de alerta: {primeras_lineas[0].strip()}")
                    if len(primeras_lineas) > 1:
                        print(f"Primera fila de alerta: {primeras_lineas[1].strip()[:100]}...")
                else:
                    print("El archivo de alertas está vacío.")
        except Exception as e:
            print(f"Error al leer archivo de alertas: {e}")
    
    if archivos_atascos_transformed:
        try:
            with open(archivos_atascos_transformed[0], 'r') as f:
                primeras_lineas = f.readlines()[:2]  # Leer encabezado y primera fila
                if primeras_lineas:
                    print(f"Encabezado de atasco: {primeras_lineas[0].strip()}")
                    if len(primeras_lineas) > 1:
                        print(f"Primera fila de atasco: {primeras_lineas[1].strip()[:100]}...")
                else:
                    print("El archivo de atascos está vacío.")
        except Exception as e:
            print(f"Error al leer archivo de atascos: {e}")

if __name__ == "__main__":
    main()