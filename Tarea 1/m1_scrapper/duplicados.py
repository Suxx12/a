import json
import os
from collections import defaultdict

def verificar_duplicados(archivo_json):
    """
    Verifica si hay UUIDs duplicados en un archivo JSON, distinguiendo
    entre alertas y atascos.
    
    Parámetros:
    archivo_json (str): Ruta al archivo JSON a analizar
    
    Retorna:
    tuple: (duplicados, total_objetos, uuids_unicos, total_alertas, total_atascos)
    """
    try:
        # Cargar el archivo JSON
        with open(archivo_json, 'r', encoding='utf-8') as f:
            datos = json.load(f)
        
        # Inicializar contador de UUIDs y sus tipos
        conteo_uuids = defaultdict(int)
        tipo_uuid = {}  # Para guardar el tipo (alerta o atasco)
        duplicados = {}
        total_objetos = 0
        total_alertas = 0
        total_atascos = 0
        
        # Buscar UUIDs en el JSON
        if isinstance(datos, dict):
            # Si el archivo tiene estructura de celdas
            if "celdas" in datos:
                for celda in datos["celdas"]:
                    # Buscar en atascos (jams)
                    if "data" in celda and "jams" in celda["data"]:
                        for jam in celda["data"]["jams"]:
                            if "uuid" in jam:
                                uuid = jam["uuid"]
                                conteo_uuids[uuid] += 1
                                if uuid not in tipo_uuid:
                                    tipo_uuid[uuid] = {"tipo": "atasco"}
                                elif tipo_uuid[uuid]["tipo"] == "alerta":
                                    tipo_uuid[uuid]["tipo"] = "mixto"  # UUID existe en ambos tipos
                                total_objetos += 1
                                total_atascos += 1
                    
                    # Buscar en alertas
                    if "data" in celda and "alerts" in celda["data"]:
                        for alert in celda["data"]["alerts"]:
                            if "uuid" in alert:
                                uuid = alert["uuid"]
                                conteo_uuids[uuid] += 1
                                if uuid not in tipo_uuid:
                                    tipo_uuid[uuid] = {"tipo": "alerta"}
                                elif tipo_uuid[uuid]["tipo"] == "atasco":
                                    tipo_uuid[uuid]["tipo"] = "mixto"  # UUID existe en ambos tipos
                                total_objetos += 1
                                total_alertas += 1
        
        # Si es una lista directa de objetos
        elif isinstance(datos, list):
            for objeto in datos:
                if "uuid" in objeto:
                    uuid = objeto["uuid"]
                    conteo_uuids[uuid] += 1
                    # Intentar determinar el tipo basado en campos presentes
                    if "roadType" in objeto and "street" in objeto:
                        tipo = "atasco"
                        total_atascos += 1
                    elif "subtype" in objeto and "reportRating" in objeto:
                        tipo = "alerta"
                        total_alertas += 1
                    else:
                        tipo = "desconocido"
                    
                    if uuid not in tipo_uuid:
                        tipo_uuid[uuid] = {"tipo": tipo}
                    elif tipo_uuid[uuid]["tipo"] != tipo and tipo != "desconocido":
                        tipo_uuid[uuid]["tipo"] = "mixto"
                    
                    total_objetos += 1
        
        # Filtrar solo los UUIDs duplicados
        for uuid, conteo in conteo_uuids.items():
            if conteo > 1:
                duplicados[uuid] = {
                    "conteo": conteo,
                    "tipo": tipo_uuid[uuid]["tipo"]
                }
        
        return duplicados, total_objetos, len(conteo_uuids), total_alertas, total_atascos
    
    except FileNotFoundError:
        print(f"Error: El archivo '{archivo_json}' no existe.")
        return {}, 0, 0, 0, 0
    except json.JSONDecodeError:
        print(f"Error: El archivo '{archivo_json}' no tiene un formato JSON válido.")
        return {}, 0, 0, 0, 0
    except Exception as e:
        print(f"Error al procesar el archivo: {str(e)}")
        return {}, 0, 0, 0, 0

def mostrar_resultados(archivo, duplicados, total_objetos, uuids_unicos, total_alertas, total_atascos):
    """Muestra los resultados del análisis de un archivo"""
    nombre_archivo = os.path.basename(archivo)
    
    if duplicados:
        print(f"\n{'=' * 20} ARCHIVO: {nombre_archivo} {'=' * 20}")
        print(f"Se encontraron {len(duplicados)} UUIDs duplicados en {total_objetos} objetos:")
        
        # Organizar duplicados por tipo
        duplicados_por_tipo = {
            "alerta": [],
            "atasco": [],
            "mixto": []
        }
        
        for uuid, info in duplicados.items():
            duplicados_por_tipo[info["tipo"]].append((uuid, info["conteo"]))
        
        # Mostrar duplicados de alertas
        if duplicados_por_tipo["alerta"]:
            print("\n--- UUIDs duplicados de ALERTAS ---")
            for uuid, conteo in duplicados_por_tipo["alerta"]:
                print(f"UUID: {uuid} - Aparece {conteo} veces")
        
        # Mostrar duplicados de atascos
        if duplicados_por_tipo["atasco"]:
            print("\n--- UUIDs duplicados de ATASCOS ---")
            for uuid, conteo in duplicados_por_tipo["atasco"]:
                print(f"UUID: {uuid} - Aparece {conteo} veces")
        
        # Mostrar UUIDs duplicados que aparecen en ambos tipos
        if duplicados_por_tipo["mixto"]:
            print("\n--- UUIDs duplicados MIXTOS (aparecen en alertas y atascos) ---")
            for uuid, conteo in duplicados_por_tipo["mixto"]:
                print(f"UUID: {uuid} - Aparece {conteo} veces")
        
        # Calcular porcentaje de duplicados
        porcentaje = (len(duplicados) / uuids_unicos) * 100 if uuids_unicos > 0 else 0
        print(f"\nResumen de {nombre_archivo}:")
        print(f"Total de objetos analizados: {total_objetos} ({total_alertas} alertas, {total_atascos} atascos)")
        print(f"UUIDs únicos: {uuids_unicos}")
        print(f"UUIDs duplicados: {len(duplicados)} ({porcentaje:.2f}%)")
    else:
        print(f"\n{'=' * 20} ARCHIVO: {nombre_archivo} {'=' * 20}")
        print(f"No se encontraron UUIDs duplicados en este archivo.")
        print(f"Total de objetos analizados: {total_objetos} ({total_alertas} alertas, {total_atascos} atascos)")
        print(f"UUIDs únicos: {uuids_unicos}")

def main():
    # Directorio donde buscar archivos JSON
    json_dir = "./jsons"
    
    # Verificar si el directorio existe
    if not os.path.isdir(json_dir):
        print(f"Error: El directorio '{json_dir}' no existe.")
        return
    
    # Buscar todos los archivos JSON en el directorio
    archivos_json = [os.path.join(json_dir, archivo) for archivo in os.listdir(json_dir) 
                    if archivo.endswith(".json")]
    
    if not archivos_json:
        print(f"No se encontraron archivos JSON en '{json_dir}'.")
        return
    
    print(f"Se encontraron {len(archivos_json)} archivos JSON para analizar.")
    
    # Estadísticas globales
    total_archivos_con_duplicados = 0
    total_duplicados_encontrados = 0
    
    # Procesar cada archivo JSON
    for archivo in archivos_json:
        duplicados, total_objetos, uuids_unicos, total_alertas, total_atascos = verificar_duplicados(archivo)
        
        # Mostrar resultados de este archivo
        mostrar_resultados(archivo, duplicados, total_objetos, uuids_unicos, total_alertas, total_atascos)
        
        # Actualizar estadísticas globales
        if duplicados:
            total_archivos_con_duplicados += 1
            total_duplicados_encontrados += len(duplicados)
    
    # Mostrar resumen global
    print(f"\n{'=' * 60}")
    print(f"RESUMEN GLOBAL:")
    print(f"Total de archivos analizados: {len(archivos_json)}")
    print(f"Archivos con duplicados: {total_archivos_con_duplicados}")
    print(f"Total de UUIDs duplicados encontrados: {total_duplicados_encontrados}")
    print(f"{'=' * 60}")

if __name__ == "__main__":
    main()