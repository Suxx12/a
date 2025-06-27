#!/usr/bin/env python3

import json
import os
from glob import glob
from collections import Counter, defaultdict

def analyze_jsons(directory=None):
    """
    Analiza archivos JSON en el directorio especificado, recorre el campo "celdas",
    extrae los UUIDs de los campos "alerts" y cuenta UUIDs repetidos.
    Muestra información adicional (subtype, type, reportby, city) para cada UUID repetido.
    """
    # Definir la ruta al directorio jsons
    if directory is None:
        directory = os.path.join("/home/renato/proyectos_universidad/sistemas_distribuidos/Tarea 1", "jsons")
    
    # Asegurarse de que el directorio existe
    if not os.path.exists(directory):
        print(f"Error: El directorio '{directory}' no existe.")
        return
    
    # Buscar todos los archivos JSON en el directorio
    json_files = glob(os.path.join(directory, "*.json"))
    
    if not json_files:
        print(f"No se encontraron archivos JSON en el directorio '{directory}'.")
        return
    
    all_uuids = []
    uuid_info = defaultdict(list)  # Para almacenar la información adicional de cada UUID
    
    # Procesar cada archivo JSON
    for json_file in json_files:
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Verificar si existe el campo "celdas"
            if "celdas" not in data:
                print(f"Advertencia: El archivo {json_file} no contiene el campo 'celdas'.")
                continue
            
            # Recorrer cada elemento en "celdas"
            for celda in data["celdas"]:
                # Verificar si existe el campo "data"
                if "data" not in celda:
                    continue
                
                # Verificar si existe el campo "alerts" en "data"
                if "alerts" not in celda["data"]:
                    continue
                
                # Extraer los UUIDs y la información adicional de "alerts"
                for alert in celda["data"]["alerts"]:
                    if "uuid" in alert:
                        uuid = alert["uuid"]
                        all_uuids.append(uuid)
                        
                        # Extraer información adicional
                        info = {
                            "subtype": alert.get("subtype", "N/A"),
                            "type": alert.get("type", "N/A"),
                            "reportby": alert.get("reportby", "N/A"),
                            "city": alert.get("city", "N/A"),
                            "file": os.path.basename(json_file)
                        }
                        
                        uuid_info[uuid].append(info)
        
        except json.JSONDecodeError:
            print(f"Error: El archivo {json_file} no es un JSON válido.")
        except Exception as e:
            print(f"Error al procesar {json_file}: {str(e)}")
    
    # Contar UUIDs 
    uuid_counter = Counter(all_uuids)
    
    # Filtrar solo los UUIDs repetidos (que aparecen más de una vez)
    repeated_uuids = {uuid: count for uuid, count in uuid_counter.items() if count > 1}
    
    # Mostrar resultados
    print(f"Resultados del análisis:")
    print(f"- Total de UUIDs encontrados: {len(all_uuids)}")
    print(f"- Total de UUIDs únicos: {len(uuid_counter)}")
    print(f"- Total de UUIDs repetidos: {len(repeated_uuids)}")
    
    if repeated_uuids:
        print("\nDetalle de UUIDs repetidos:")
        for uuid, count in sorted(repeated_uuids.items(), key=lambda x: x[1], reverse=True):
            print(f"\n- UUID: {uuid}, aparece {count} veces:")
            
            # Mostrar información adicional para cada instancia del UUID
            for idx, info in enumerate(uuid_info[uuid], 1):
                print(f"  Instancia {idx}:")
                print(f"    Subtype: {info['subtype']}")
                print(f"    Type: {info['type']}")
                print(f"    Reportby: {info['reportby']}")
                print(f"    City: {info['city']}")
                print(f"    Archivo: {info['file']}")
    else:
        print("\nNo se encontraron UUIDs repetidos.")
    
    # Devolver el número de UUIDs repetidos
    return len(repeated_uuids)

if __name__ == "__main__":
    analyze_jsons()