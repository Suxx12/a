import requests
import json
import time
import os
from datetime import datetime
from pymongo import MongoClient

def generar_parametros_santiago(indice, total_divisiones=32):
    """
    Genera parámetros de coordenadas para un bounding box que cubre Santiago,
    dividiendo el área en una cuadrícula para cubrir toda el área sin solapamiento.
    
    Args:
        indice: Índice de la división (0 a total_divisiones-1)
        total_divisiones: Número total de divisiones (por defecto 32 = 8x4)
        
    Retorna un diccionario con: top, bottom, left, right y metadatos.
    """
    # Coordenadas del área específica de Santiago
    norte_lat = -33.20000  # Superior izquierda (latitud)
    sur_lat = -33.70686    # Inferior derecha (latitud)
    oeste_lon = -71.00000  # Superior izquierda (longitud)
    este_lon = -70.46603   # Inferior derecha (longitud)
    
    # Para una cuadrícula de 32 celdas, usamos 8x4 (más celdas horizontalmente)
    filas = 4
    columnas = 8
    
    if total_divisiones != filas * columnas:
        raise ValueError(f"Esta función está configurada para una cuadrícula de {filas}x{columnas} ({filas*columnas} celdas)")
    
    # Validamos el índice
    if indice < 0 or indice >= total_divisiones:
        raise ValueError(f"Índice debe estar entre 0 y {total_divisiones-1}")
    
    # Calculamos las dimensiones de cada celda
    ancho_total = este_lon - oeste_lon
    altura_total = norte_lat - sur_lat
    
    ancho_celda = ancho_total / columnas
    altura_celda = altura_total / filas
    
    # Calculamos la fila y columna en la cuadrícula
    fila = indice // columnas  # De norte a sur
    columna = indice % columnas  # De oeste a este
    
    # Calculamos las coordenadas de esta celda
    top = norte_lat - (fila * altura_celda)
    bottom = top - altura_celda
    left = oeste_lon + (columna * ancho_celda)
    right = left + ancho_celda
    
    # Zonas de Santiago para identificación aproximada (32 zonas)
    zonas_santiago = [
        # Fila 1
        "Quilicura", "Huechuraba", "Recoleta", "Providencia", 
        "Las Condes", "Lo Barnechea", "La Dehesa", "Chicureo",
        # Fila 2
        "Pudahuel", "Cerro Navia", "Santiago", "Ñuñoa", 
        "La Reina", "Peñalolén", "La Florida", "Puente Alto Norte",
        # Fila 3
        "Maipú Norte", "Estación Central", "San Miguel", "San Joaquín", 
        "Macul", "La Florida Oeste", "Puente Alto Oeste", "Puente Alto Este",
        # Fila 4
        "Maipú Sur", "Cerrillos", "Lo Espejo", "La Cisterna", 
        "La Granja", "La Pintana", "El Bosque", "San Bernardo"
    ]
    
    # Asignar zona aproximada (esto es solo referencial)
    if len(zonas_santiago) >= total_divisiones:
        zona_nombre = zonas_santiago[indice]
    else:
        zona_nombre = f"Zona Santiago {indice+1}"
    
    return {
        "top": top,
        "bottom": bottom,
        "left": left,
        "right": right,
        "zona_aproximada": zona_nombre,
        "indice_celda": indice,
        "total_celdas": total_divisiones,
        "fila": fila + 1,
        "columna": columna + 1,
        "grid_size": f"{columnas}x{filas}"
    }

def get_waze_data(coord_params):
    """
    Obtiene datos de Waze para las coordenadas especificadas
    """
    # URL with the parameters for the Waze API
    url = "https://www.waze.com/live-map/api/georss"
    
    # Parameters for the request
    params = {
        'top': coord_params['top'],
        'bottom': coord_params['bottom'],
        'left': coord_params['left'],
        'right': coord_params['right'],
        'env': 'row',
        'types': 'alerts,traffic,users'  # Obtenemos alertas, tráfico y usuarios
    }
    
    # Adding headers to mimic a browser request
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        # Make the GET request
        response = requests.get(url, params=params, headers=headers)
        
        # Check if the request was successful
        if response.status_code == 200:
            # Parse the JSON response
            data = response.json()
            
            # Add location metadata to the data
            data['location_metadata'] = {
                'zona_aproximada': coord_params['zona_aproximada'],
                'indice_celda': coord_params['indice_celda'],
                'total_celdas': coord_params['total_celdas'],
                'fila': coord_params['fila'],
                'columna': coord_params['columna'],
                'grid': coord_params['grid_size'],
                'coord_bbox': {
                    'top': coord_params['top'],
                    'bottom': coord_params['bottom'],
                    'left': coord_params['left'],
                    'right': coord_params['right']
                }
            }
            
            # Print summary
            print(f"\nDatos para zona aproximada: {coord_params['zona_aproximada']} (Celda {coord_params['indice_celda']+1}/{coord_params['total_celdas']})")
            print(f"- Cuadrícula: Fila {coord_params['fila']}, Columna {coord_params['columna']} (Grid {coord_params['grid_size']})")
            print(f"- Coordenadas: {coord_params['top']:.4f}° a {coord_params['bottom']:.4f}° (lat), {coord_params['left']:.4f}° a {coord_params['right']:.4f}° (lon)")
            
            if 'alerts' in data:
                print(f"- Número de alertas: {len(data['alerts'])}")
            if 'jams' in data:
                print(f"- Número de atascos: {len(data['jams'])}")
            if 'users' in data:
                print(f"- Número de usuarios: {len(data['users'])}")
                
            return data
        else:
            print(f"La petición falló con código: {response.status_code}")
            print(f"Respuesta: {response.text}")
            return None
            
    except Exception as e:
        print(f"Ocurrió un error: {e}")
        return None


if __name__ == "__main__":
    print("=== WAZE DATA SCRAPER PARA SANTIAGO (ÁREA ESPECÍFICA) ===")
    
    # Conectar a MongoDB
    try:
        client = MongoClient('mongodb://admin:admin123@mongo:27017/')
        db = client['trafico_rm']  # Nombre de la base de datos
        alertas_collection = db['alertas']
        atascos_collection = db['atascos']
        print("Conexión establecida con MongoDB")
    except Exception as e:
        print(f"Error al conectar con MongoDB: {e}")
        exit(1)
    
    # Definimos 32 celdas para el área específica de Santiago
    total_celdas = 32  # Cuadrícula de 8x4 (más amplia horizontalmente)
    filas = 4
    columnas = 8
    
    print(f"\nRealizando {total_celdas} consultas en cuadrícula {columnas}x{filas} para cubrir Santiago...")
    print("Sin solapamiento entre áreas para evitar duplicados")
    print("Área delimitada por:")
    print("- Superior izquierda: 33°12'00.0\"S 71°00'00.0\"W (-33.20000, -71.00000)")
    print("- Superior derecha:   33°12'12.3\"S 70°30'21.4\"W (-33.20342, -70.50594)")
    print("- Inferior izquierda: 33°39'11.0\"S 70°59'24.8\"W (-33.65306, -70.99022)")
    print("- Inferior derecha:   33°42'24.7\"S 70°27'57.7\"W (-33.70686, -70.46603)")
    
    # Create 'jsons' directory if it doesn't exist
    jsons_dir = 'jsons'
    if not os.path.exists(jsons_dir):
        os.makedirs(jsons_dir)
        print(f"Directorio creado: {jsons_dir}")
    
    # Timestamp for this batch of requests
    batch_timestamp = time.strftime("%Y%m%d-%H%M%S")
    current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    region = "Santiago"
    
    # Estructura simplificada que solo contendrá las alertas y atascos
    resultado_simplificado = {
        "celdas": []
    }
    
    # Contadores para estadísticas
    total_alertas = 0
    total_atascos = 0
    total_alertas_mongodb = 0
    total_atascos_mongodb = 0
    
    # Generate parameters and fetch data for each cell in the grid
    for i in range(total_celdas):
        print(f"\nConsultando celda {i+1}/{total_celdas} (Cuadrícula {columnas}x{filas})...")
        
        # Generate parameters for this cell of the Santiago grid
        coords = generar_parametros_santiago(i, total_celdas)
        
        # Get Waze data for these coordinates
        data = get_waze_data(coords)
        
        # Procesamos los datos y extraemos solo lo necesario
        if data:
            celda = {
                "data": {
                    "alerts": [],
                    "jams": []
                }
            }
            
            # Procesamos las alertas y añadimos fecha y región
            alertas_para_mongodb = []
            if 'alerts' in data:
                for alert in data['alerts']:
                    # Añadimos la fecha y región a cada alerta
                    alert['fecha'] = current_date
                    alert['region'] = region
                    celda["data"]["alerts"].append(alert)
                    alertas_para_mongodb.append(alert)
                
                num_alertas = len(data['alerts'])
                total_alertas += num_alertas
                
                # Insertar alertas en MongoDB
                if alertas_para_mongodb:
                    try:
                        resultado_insert = alertas_collection.insert_many(alertas_para_mongodb)
                        total_alertas_mongodb += len(resultado_insert.inserted_ids)
                        print(f"Se insertaron {len(resultado_insert.inserted_ids)} alertas en MongoDB")
                    except Exception as e:
                        print(f"Error al insertar alertas en MongoDB: {e}")
            else:
                num_alertas = 0
            
            # Procesamos los atascos y añadimos fecha y región
            atascos_para_mongodb = []
            if 'jams' in data:
                for jam in data['jams']:
                    # Añadimos la fecha y región a cada atasco
                    jam['fecha'] = current_date
                    jam['region'] = region
                    celda["data"]["jams"].append(jam)
                    atascos_para_mongodb.append(jam)
                
                num_atascos = len(data['jams'])
                total_atascos += num_atascos
                
                # Insertar atascos en MongoDB
                if atascos_para_mongodb:
                    try:
                        resultado_insert = atascos_collection.insert_many(atascos_para_mongodb)
                        total_atascos_mongodb += len(resultado_insert.inserted_ids)
                        print(f"Se insertaron {len(resultado_insert.inserted_ids)} atascos en MongoDB")
                    except Exception as e:
                        print(f"Error al insertar atascos en MongoDB: {e}")
            else:
                num_atascos = 0
            
            # Solo añadimos la celda si contiene alertas o atascos
            if num_alertas > 0 or num_atascos > 0:
                resultado_simplificado["celdas"].append(celda)
                print(f"Celda {i+1} procesada con éxito: {num_alertas} alertas, {num_atascos} atascos")
            else:
                print(f"Celda {i+1} sin alertas ni atascos, omitida en el resultado final")
        
        # Add a small delay between requests to avoid rate limiting
        if i < total_celdas - 1:
            time.sleep(2)  # Tiempo de espera entre peticiones
    
    # Guardamos todo en un solo archivo JSON
    #filename = f"{jsons_dir}/waze_santiago_{batch_timestamp}.json"
    #with open(filename, 'w') as f:
    #   json.dump(resultado_simplificado, f, indent=4)
    
    print("\n=== SCRAPING COMPLETADO ===")
    print(f"Se consultaron {total_celdas} celdas en Santiago en una cuadrícula {columnas}x{filas}")
    #print(f"Resultados guardados en: {filename}")
    print(f"Total de alertas recopiladas: {total_alertas}")
    print(f"Total de atascos recopilados: {total_atascos}")
    print(f"Total de alertas insertadas en MongoDB: {total_alertas_mongodb}")
    print(f"Total de atascos insertados en MongoDB: {total_atascos_mongodb}")