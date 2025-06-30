from pymongo import MongoClient
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from datetime import datetime
import pandas as pd
import os
import glob

def export_mongo_to_elasticsearch():
    """Exporta directamente desde MongoDB a Elasticsearch usando bulk operations"""
    
    print("=== EXPORTANDO MONGO -> ELASTICSEARCH (BULK) ===")
    
    # Conectar a MongoDB
    try:
        mongo_client = MongoClient('mongodb://admin:admin123@mongo:27017/')
        db = mongo_client['trafico_rm']
        mongo_client.admin.command('ping')
        print("✓ Conectado a MongoDB")
    except Exception as e:
        print(f"✗ Error conectando a MongoDB: {e}")
        return False
    
    # Conectar a Elasticsearch
    try:
        es = Elasticsearch("http://elasticsearch:9200")
        if not es.ping():
            raise Exception("No se pudo conectar a Elasticsearch")
        print("✓ Conectado a Elasticsearch")
    except Exception as e:
        print(f"✗ Error conectando a Elasticsearch: {e}")
        return False
    
    # Función auxiliar para preparar documentos
    def prepare_docs_for_bulk(collection, index_name):
        """Prepara documentos para bulk insert"""
        for doc in collection.find({}):
            # Extraer el _id y eliminarlo del documento
            doc_id = str(doc.pop('_id'))
            
            yield {
                "_index": index_name,
                "_id": doc_id,
                "_source": doc
            }
    
    # Exportar alertas
    try:
        alertas_collection = db['alertas']
        alertas_count = alertas_collection.count_documents({})
        print(f"\n📊 Procesando {alertas_count} alertas...")
        
        # Usar bulk helper
        success, failed = bulk(
            es,
            prepare_docs_for_bulk(alertas_collection, "waze_alertas"),
            chunk_size=1000,  # Procesar en lotes de 1000
            request_timeout=60
        )
        
        print(f"✓ {success} alertas insertadas exitosamente")
        if failed:
            print(f"⚠ {len(failed)} alertas fallaron")
        
    except Exception as e:
        print(f"✗ Error procesando alertas: {e}")
    
    # Exportar atascos
    try:
        atascos_collection = db['atascos']
        atascos_count = atascos_collection.count_documents({})
        print(f"\n📊 Procesando {atascos_count} atascos...")
        
        # Usar bulk helper
        success, failed = bulk(
            es,
            prepare_docs_for_bulk(atascos_collection, "waze_atascos"),
            chunk_size=1000,  # Procesar en lotes de 1000
            request_timeout=60
        )
        
        print(f"✓ {success} atascos insertados exitosamente")
        if failed:
            print(f"⚠ {len(failed)} atascos fallaron")
        
    except Exception as e:
        print(f"✗ Error procesando atascos: {e}")
    
    # Cerrar conexiones
    mongo_client.close()
    print("\n=== EXPORTACIÓN MONGO COMPLETADA ===")
    return True

def export_csv_to_elasticsearch():
    """Exporta archivos CSV procesados a Elasticsearch"""
    
    print("\n=== EXPORTANDO CSV -> ELASTICSEARCH ===")
    
    # Conectar a Elasticsearch
    try:
        es = Elasticsearch("http://elasticsearch:9200")
        if not es.ping():
            raise Exception("No se pudo conectar a Elasticsearch")
        print("✓ Conectado a Elasticsearch")
    except Exception as e:
        print(f"✗ Error conectando a Elasticsearch: {e}")
        return False
    
    # Encontrar la carpeta de ejecución más reciente
    try:
        results_path = "/app/input"  # Carpeta montada desde m8-processing/results
        print(f"🔍 Buscando en: {results_path}")
        
        # Buscar carpetas que empiecen con "ejecucion_"
        ejecucion_folders = glob.glob(os.path.join(results_path, "ejecucion_*"))
        
        if not ejecucion_folders:
            print(f"✗ No se encontraron carpetas de ejecución en {results_path}")
            return False
        
        # Ordenar por nombre (incluye timestamp, así que funciona)
        latest_folder = max(ejecucion_folders)
        print(f"✓ Usando carpeta más reciente: {os.path.basename(latest_folder)}")
        
    except Exception as e:
        print(f"✗ Error buscando carpeta de ejecución: {e}")
        return False
    
    # Buscar todos los archivos CSV en la carpeta
    try:
        csv_files = glob.glob(os.path.join(latest_folder, "*.csv"))
        
        if not csv_files:
            print(f"✗ No se encontraron archivos CSV en {latest_folder}")
            return False
        
        print(f"✓ Encontrados {len(csv_files)} archivos CSV")
        
    except Exception as e:
        print(f"✗ Error buscando archivos CSV: {e}")
        return False
    
    # Función para preparar documentos CSV para bulk
    def prepare_csv_for_bulk(csv_file_path):
        """Convierte CSV a documentos para Elasticsearch"""
        try:
            # Leer CSV
            df = pd.read_csv(csv_file_path)
            
            # Obtener nombre del archivo sin extensión
            file_name = os.path.splitext(os.path.basename(csv_file_path))[0]
            
            # Crear índice específico para cada tipo de análisis
            index_name = f"analisis_{file_name}"
            
            print(f"  📋 {file_name}.csv -> índice: {index_name} ({len(df)} filas)")
            
            # Convertir cada fila a documento
            for idx, row in df.iterrows():
                doc = row.to_dict()
                
                # Agregar metadatos
                doc['archivo_origen'] = file_name
                doc['fecha_procesamiento'] = datetime.now().isoformat()
                doc['fila_numero'] = idx + 1
                
                # Usar combinación de archivo y fila como ID único
                doc_id = f"{file_name}_{idx}"
                
                yield {
                    "_index": index_name,
                    "_id": doc_id,
                    "_source": doc
                }
                
        except Exception as e:
            print(f"  ✗ Error procesando {csv_file_path}: {e}")
            return
    
    # Procesar cada archivo CSV
    total_success = 0
    total_failed = 0
    indices_creados = []
    
    for csv_file in csv_files:
        try:
            file_name = os.path.basename(csv_file)
            
            # Usar bulk helper
            success, failed = bulk(
                es,
                prepare_csv_for_bulk(csv_file),
                chunk_size=1000,
                request_timeout=60
            )
            
            print(f"  ✓ {success} registros insertados")
            if failed:
                print(f"  ⚠ {len(failed)} registros fallaron")
            
            total_success += success
            total_failed += len(failed) if failed else 0
            
            # Agregar índice a la lista
            index_name = f"analisis_{os.path.splitext(file_name)[0]}"
            indices_creados.append(index_name)
            
        except Exception as e:
            print(f"  ✗ Error procesando {file_name}: {e}")
    
    print(f"\n=== EXPORTACIÓN CSV COMPLETADA ===")
    print(f"📊 Total registros insertados: {total_success}")
    print(f"⚠ Total registros fallidos: {total_failed}")
    print(f"📁 Índices creados: {len(indices_creados)}")
    for idx in indices_creados:
        print(f"   - {idx}")
    
    return True

if __name__ == "__main__":
    # Va a mongo y exporta a elasticsearch
    while(True): 
        sleep(10)  # Espera a que los servicios estén listos
        export_mongo_to_elasticsearch()
    
        # Va a los csv de insights y exporta a elasticsearch
        export_csv_to_elasticsearch()