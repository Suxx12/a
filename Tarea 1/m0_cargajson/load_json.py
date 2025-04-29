import os
import json
import glob
from pymongo import MongoClient
from bson import ObjectId
import time

# Configuraciones desde variables de entorno
MONGODB_URI = os.environ.get('MONGODB_URI', 'mongodb://admin:admin123@mongo:27017/')
MONGODB_DB = os.environ.get('MONGODB_DB', 'trafico_rm')
MONGODB_COLECCION_ALERTAS = os.environ.get('MONGODB_COLECCION_ALERTAS', 'alertas')
MONGODB_COLECCION_ATASCOS = os.environ.get('MONGODB_COLECCION_ATASCOS', 'atascos')

def cargar_json_en_mongodb(archivo_json):
    """
    Carga datos de un archivo JSON de Waze a MongoDB
    """
    try:
        # Leer el archivo JSON
        with open(archivo_json, 'r') as file:
            print(f"Leyendo archivo: {archivo_json}")
            data = json.load(file)
        
        # Procesar datos del JSON
        total_alertas = 0
        total_atascos = 0
        alertas_para_mongodb = []
        atascos_para_mongodb = []
        
        # Extraer todas las alertas y atascos de las celdas
        for celda in data.get('celdas', []):
            if 'data' in celda:
                # Extraer alertas
                for alert in celda['data'].get('alerts', []):
                    alertas_para_mongodb.append(alert)
                    total_alertas += 1
                
                # Extraer atascos (jams)
                for jam in celda['data'].get('jams', []):
                    atascos_para_mongodb.append(jam)
                    total_atascos += 1
        
        # Conectar a MongoDB e insertar datos
        client = MongoClient(MONGODB_URI)
        db = client[MONGODB_DB]
        alertas_collection = db[MONGODB_COLECCION_ALERTAS]
        atascos_collection = db[MONGODB_COLECCION_ATASCOS]
        
        # Insertar alertas en MongoDB
        alertas_insertadas = 0
        if alertas_para_mongodb:
            try:
                resultado = alertas_collection.insert_many(alertas_para_mongodb, ordered=False)
                alertas_insertadas = len(resultado.inserted_ids)
                print(f"Se insertaron {alertas_insertadas} alertas en MongoDB")
                
                # Calcula duplicados
                duplicados = total_alertas - alertas_insertadas
                if duplicados > 0:
                    print(f"Nota: {duplicados} alertas duplicadas fueron ignoradas")
                
            except Exception as e:
                if "duplicate key error" in str(e):
                    print(f"Error: Algunas o todas las alertas ya existían en la base de datos")
                    # Intentamos extraer el número de documentos insertados del error
                    try:
                        # Esto es aproximado y puede no funcionar con todas las versiones de MongoDB
                        if hasattr(e, 'details') and 'nInserted' in e.details:
                            alertas_insertadas = e.details['nInserted']
                            print(f"Se pudieron insertar aproximadamente {alertas_insertadas} alertas")
                    except:
                        pass
                else:
                    print(f"Error al insertar alertas en MongoDB: {e}")
        
        # Insertar atascos en MongoDB
        atascos_insertados = 0
        if atascos_para_mongodb:
            try:
                resultado = atascos_collection.insert_many(atascos_para_mongodb, ordered=False)
                atascos_insertados = len(resultado.inserted_ids)
                print(f"Se insertaron {atascos_insertados} atascos en MongoDB")
                
                # Calcula duplicados
                duplicados = total_atascos - atascos_insertados
                if duplicados > 0:
                    print(f"Nota: {duplicados} atascos duplicados fueron ignorados")
                
            except Exception as e:
                if "duplicate key error" in str(e):
                    print(f"Error: Algunos o todos los atascos ya existían en la base de datos")
                    # Intentamos extraer el número de documentos insertados del error
                    try:
                        if hasattr(e, 'details') and 'nInserted' in e.details:
                            atascos_insertados = e.details['nInserted']
                            print(f"Se pudieron insertar aproximadamente {atascos_insertados} atascos")
                    except:
                        pass
                else:
                    print(f"Error al insertar atascos en MongoDB: {e}")
        
        print(f"\nResumen de la carga del archivo {os.path.basename(archivo_json)}:")
        print(f"- Alertas encontradas en el JSON: {total_alertas}")
        print(f"- Atascos encontrados en el JSON: {total_atascos}")
        print(f"- Alertas insertadas en MongoDB: {alertas_insertadas}")
        print(f"- Atascos insertados en MongoDB: {atascos_insertados}")
        
        return True, alertas_insertadas, atascos_insertados
        
    except FileNotFoundError:
        print(f"Error: El archivo {archivo_json} no existe")
        return False, 0, 0
    except json.JSONDecodeError:
        print(f"Error: El archivo {archivo_json} no tiene un formato JSON válido")
        return False, 0, 0
    except Exception as e:
        print(f"Error al procesar el archivo {archivo_json}: {e}")
        return False, 0, 0

def main():
    # Directorio donde se buscarán los archivos JSON
    jsons_dir = '/app/jsons'
    
    # Verificar que el directorio exista
    if not os.path.exists(jsons_dir):
        print(f"Error: El directorio {jsons_dir} no existe")
        return
    
    # Obtener lista de archivos JSON
    archivos_json = glob.glob(os.path.join(jsons_dir, '*.json'))
    
    if not archivos_json:
        print(f"No se encontraron archivos JSON en {jsons_dir}")
        return
    
    print(f"Se encontraron {len(archivos_json)} archivos JSON para procesar")
    
    # Conectar a MongoDB y crear índices
    try:
        client = MongoClient(MONGODB_URI)
        db = client[MONGODB_DB]
        alertas_collection = db[MONGODB_COLECCION_ALERTAS]
        atascos_collection = db[MONGODB_COLECCION_ATASCOS]
        
        # Crear índices únicos en el campo 'uuid' si no existen
        alertas_collection.create_index(
            [("uuid", 1)],  # Índice en la columna 'uuid'
            unique=True,
            name="unique_alert_uuid"
        )
        atascos_collection.create_index(
            [("uuid", 1)],  # Índice en la columna 'uuid'
            unique=True,
            name="unique_jam_uuid"
        )
        print("Índices UUID creados/verificados en MongoDB")
    except Exception as e:
        print(f"Error al conectar con MongoDB o crear índices: {e}")
        return
    
    # Contadores totales
    total_alertas_insertadas = 0
    total_atascos_insertados = 0
    archivos_procesados = 0
    
    # Procesar cada archivo
    for archivo in archivos_json:
        print(f"\nProcesando archivo {archivos_procesados+1}/{len(archivos_json)}: {os.path.basename(archivo)}")
        resultado, alertas, atascos = cargar_json_en_mongodb(archivo)
        
        if resultado:
            archivos_procesados += 1
            total_alertas_insertadas += alertas
            total_atascos_insertados += atascos
            
            # Esperar un segundo entre archivos para no sobrecargar MongoDB
            if archivos_procesados < len(archivos_json):
                time.sleep(1)
    
    print("\n====== RESUMEN FINAL ======")
    print(f"Archivos procesados: {archivos_procesados}/{len(archivos_json)}")
    print(f"Total alertas insertadas: {total_alertas_insertadas}")
    print(f"Total atascos insertados: {total_atascos_insertados}")

if __name__ == "__main__":
    main()