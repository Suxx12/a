import requests
import time
from datetime import datetime
from pymongo import MongoClient

def generar_parametros_santiago(indice, total_divisiones=32):
    """Genera coordenadas para dividir Santiago en una cuadrícula"""
    # Coordenadas límite de Santiago
    norte_lat, sur_lat = -33.20000, -33.70686
    oeste_lon, este_lon = -71.00000, -70.46603
    
    # Configuración de la cuadrícula
    filas, columnas = 4, 8
    
    # Valores predefinidos para tamaño de celdas
    ancho_celda = 0.0667  # (este_lon - oeste_lon) / columnas
    altura_celda = 0.1267  # (norte_lat - sur_lat) / filas
    
    # Calcular posición
    fila = indice // columnas
    columna = indice % columnas
    
    # Calcular coordenadas
    top = norte_lat - (fila * altura_celda)
    bottom = top - altura_celda
    left = oeste_lon + (columna * ancho_celda)
    right = left + ancho_celda
    
    return {
        "top": top, "bottom": bottom, "left": left, "right": right,
        "zona_aproximada": f"Celda {indice+1}", "indice_celda": indice,
        "total_celdas": total_divisiones, "fila": fila + 1, 
        "columna": columna + 1, "grid_size": f"{columnas}x{filas}"
    }

def get_waze_data(coord_params):
    """Obtiene datos de Waze para las coordenadas especificadas"""
    url = "https://www.waze.com/live-map/api/georss"
    
    params = {
        'top': coord_params['top'],
        'bottom': coord_params['bottom'],
        'left': coord_params['left'],
        'right': coord_params['right'],
        'env': 'row',
        'types': 'alerts,traffic,users'
    }
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        response = requests.get(url, params=params, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            
            # Añadir metadatos de ubicación
            data['location_metadata'] = {
                'zona_aproximada': coord_params['zona_aproximada'],
                'indice_celda': coord_params['indice_celda'],
                'total_celdas': coord_params['total_celdas'],
                'fila': coord_params['fila'],
                'columna': coord_params['columna'],
                'grid': coord_params['grid_size'],
                'coord_bbox': {
                    'top': coord_params['top'], 'bottom': coord_params['bottom'],
                    'left': coord_params['left'], 'right': coord_params['right']
                }
            }
            
            return data
        else:
            return None
            
    except Exception as e:
        return None


if __name__ == "__main__":
    print("=== WAZE DATA SCRAPER PARA SANTIAGO ===")
    
    # Conectar a MongoDB
    try:
        client = MongoClient('mongodb://admin:admin123@mongo:27017/')
        db = client['trafico_rm']
        alertas_collection = db['alertas']
        atascos_collection = db['atascos']
        
        # Crear índices 
        try:
            # Crear o verificar índice para alertas
            indices_alertas = alertas_collection.index_information()
            if 'unique_alert_uuid' not in indices_alertas:
                alertas_collection.create_index([("uuid", 1)], unique=True, name='unique_alert_uuid')
              
                
            # Crear o verificar índice para atascos
            indices_atascos = atascos_collection.index_information()
            if 'unique_atasco_uuid' not in indices_atascos:
                atascos_collection.create_index([("uuid", 1)], unique=True, name='unique_atasco_uuid')
                
        except Exception as e:
            print(f"Advertencia al configurar índices: {e}")
        
        print("Conexión establecida con MongoDB")
    except Exception as e:
        print(f"Error al conectar con MongoDB: {e}")
        exit(1)
    
    # Configuración
    total_celdas, filas, columnas = 32, 4, 8
    current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    region = "Santiago"
    
    # Contadores
    total_alertas = total_atascos = total_alertas_mongodb = total_atascos_mongodb = 0
    
    print(f"\nConsultando {total_celdas} celdas en cuadrícula {columnas}x{filas}")
    print("Coordenadas límite: (-33.20000, -71.00000) a (-33.70686, -70.46603)")
    
    # Recorrer cuadrícula
    for i in range(total_celdas):
        print(f"\nConsultando celda {i+1}/{total_celdas}...")
        
        # Obtener datos
        coords = generar_parametros_santiago(i, total_celdas)
        data = get_waze_data(coords)
        
        if not data:
            continue
            
        celda = {"data": {"alerts": [], "jams": []}}
        
        # Procesar alertas
        if 'alerts' in data:
            alertas_para_mongodb = []
            for alert in data['alerts']:
                alert['fecha'] = current_date
                alert['region'] = region
                celda["data"]["alerts"].append(alert)
                alertas_para_mongodb.append(alert)
            
            num_alertas = len(alertas_para_mongodb)
            total_alertas += num_alertas
            
            if num_alertas > 0:
                print(f"Encontradas {num_alertas} alertas")
                
            if alertas_para_mongodb:
                try:
                    resultado = alertas_collection.insert_many(alertas_para_mongodb, ordered=False)
                    insertados = len(resultado.inserted_ids)
                    total_alertas_mongodb += insertados
                except Exception:
                    # Error silencioso (no muestra mensaje de error)
                    pass
        else:
            num_alertas = 0
        
        # Procesar atascos
        if 'jams' in data:
            atascos_para_mongodb = []
            for jam in data['jams']:
                jam['fecha'] = current_date
                jam['region'] = region
                celda["data"]["jams"].append(jam)
                atascos_para_mongodb.append(jam)
            
            num_atascos = len(atascos_para_mongodb)
            total_atascos += num_atascos
            
            if num_atascos > 0:
                print(f"Encontrados {num_atascos} atascos")
                
            if atascos_para_mongodb:
                try:
                    resultado = atascos_collection.insert_many(atascos_para_mongodb, ordered=False)
                    insertados = len(resultado.inserted_ids)
                    total_atascos_mongodb += insertados
                except Exception:
                    # Error silencioso (no muestra mensaje de error)
                    pass
        else:
            num_atascos = 0
        
        # Esperar entre peticiones
        if i < total_celdas - 1:
            time.sleep(2)
    
    # Resumen final
    print("\n=== SCRAPING COMPLETADO ===")
    print(f"Se consultaron {total_celdas} celdas en Santiago")
    print(f"Total de alertas: {total_alertas}")
    print(f"Total de atascos: {total_atascos}")
    print("Se insertaron a la base de datos no repetidos")