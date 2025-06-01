#!/usr/bin/env python3
import json
import os
import glob
import sys
from datetime import datetime

def procesar_alertas(archivo_entrada, archivo_salida):
    """
    Procesa un archivo JSON de alertas y extrae los campos específicos requeridos.
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
                    
                    with open(archivo_salida, 'w') as f_out:
                        for uuid, alerta in datos.items():
                            # Crear un objeto con los campos requeridos específicos para alertas
                            resultado = {
                                "uuid": alerta.get("uuid", uuid),
                                "nThumbsUp": alerta.get("nThumbsUp", 0),
                                "city": alerta.get("city", ""),
                                "municipalityUser": alerta.get("municipalityUser", ""),
                                "type": alerta.get("type", ""),
                                "subtype": alerta.get("subtype", ""),
                                "street": alerta.get("street", ""),
                                "reportedBy": alerta.get("reportedBy", ""),
                                "confidence": alerta.get("confidence", 0),
                                "location_x": alerta.get("location", {}).get("x", alerta.get("x", 0)),
                                "location_y": alerta.get("location", {}).get("y", alerta.get("y", 0)),
                                "fecha": alerta.get("fecha", "")
                            }
                            
                            # Escribir el resultado como una línea JSON
                            f_out.write(json.dumps(resultado) + '\n')
                    
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
    """
    print(f"Procesando atascos en {archivo_entrada}...")
    
    try:
        with open(archivo_entrada, 'r') as f:
            try:
                # Cargar todo el archivo como un único objeto JSON
                datos = json.load(f)
                
                # Verificar si es un diccionario donde cada clave es un UUID
                if isinstance(datos, dict) and all(isinstance(datos.get(k), dict) for k in datos if isinstance(k, str)):
                    print(f"Formato detectado: diccionario de atascos. Procesando {len(datos)} registros...")
                    
                    with open(archivo_salida, 'w') as f_out:
                        for uuid, atasco in datos.items():
                            # Crear un objeto con los campos requeridos específicos para atascos
                            resultado = {
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
                                "city": atasco.get("city", "")
                            }
                            
                            # Escribir el resultado como una línea JSON
                            f_out.write(json.dumps(resultado) + '\n')
                    
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
        nombre_salida = os.path.join(dir_salida, f"transformed_alerta_{nombre_base_sin_extension}.jsonl")
        
        if procesar_alertas(archivo, nombre_salida):
            alertas_procesadas += 1
    
    # Procesar archivos de atascos
    atascos_procesados = 0
    for archivo in archivos_atascos:
        nombre_base = os.path.basename(archivo)
        nombre_base_sin_extension = os.path.splitext(nombre_base)[0]
        nombre_salida = os.path.join(dir_salida, f"transformed_atasco_{nombre_base_sin_extension}.jsonl")
        
        if procesar_atascos(archivo, nombre_salida):
            atascos_procesados += 1
    
    # Mostrar resumen del procesamiento
    print(f"Proceso completado.")
    print(f"- Alertas procesadas: {alertas_procesadas}/{len(archivos_alertas)}")
    print(f"- Atascos procesados: {atascos_procesados}/{len(archivos_atascos)}")
    
    # Verificar que los archivos se hayan creado correctamente
    archivos_alertas_transformed = glob.glob(os.path.join(dir_salida, "transformed_alerta_*.jsonl"))
    archivos_atascos_transformed = glob.glob(os.path.join(dir_salida, "transformed_atasco_*.jsonl"))
    
    print(f"Archivos de alertas creados: {len(archivos_alertas_transformed)}")
    print(f"Archivos de atascos creados: {len(archivos_atascos_transformed)}")
    
    # Intentar leer un archivo transformado para verificar
    if archivos_alertas_transformed:
        try:
            with open(archivos_alertas_transformed[0], 'r') as f:
                primera_linea = f.readline().strip()
                if primera_linea:
                    print(f"Primera línea de alerta: {primera_linea[:100]}...")
                else:
                    print("El archivo de alertas está vacío.")
        except Exception as e:
            print(f"Error al leer archivo de alertas: {e}")
    
    if archivos_atascos_transformed:
        try:
            with open(archivos_atascos_transformed[0], 'r') as f:
                primera_linea = f.readline().strip()
                if primera_linea:
                    print(f"Primera línea de atasco: {primera_linea[:100]}...")
                else:
                    print("El archivo de atascos está vacío.")
        except Exception as e:
            print(f"Error al leer archivo de atascos: {e}")

if __name__ == "__main__":
    main()