import requests
import json
import time
import os
import numpy as np
from datetime import datetime
import folium
import webbrowser
from tempfile import NamedTemporaryFile

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
    # Estas son las coordenadas convertidas a decimales de los puntos proporcionados
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

def visualizar_cuadricula(total_celdas=32):
    """
    Crea un mapa interactivo mostrando las cuadrículas que se utilizarán para el scraping.
    
    Args:
        total_celdas: Número total de celdas para dividir la región
        
    Returns:
        La ruta al archivo HTML del mapa
    """
    # Crear un mapa centrado en Santiago
    m = folium.Map(location=[-33.45, -70.67], zoom_start=10)
    
    # Información de la cuadrícula
    filas = 4
    columnas = 8
    print(f"\nGenerando visualización del mapa con cuadrícula {columnas}x{filas} (32 celdas)...")
    
    # Colores para las celdas en el mapa
    colors = ['blue', 'red', 'green', 'orange', 'purple', 'darkblue', 'darkred', 'beige', 'darkgreen', 'cadetblue']
    
    # Mostrar el límite del área completa
    try:
        # Primera celda para obtener parámetros iniciales
        params_completos = generar_parametros_santiago(0, total_celdas)
        norte_lat = params_completos['top']
        oeste_lon = params_completos['left']
        
        # Última celda para obtener los límites inferiores
        params_completos = generar_parametros_santiago(total_celdas-1, total_celdas)
        sur_lat = params_completos['bottom']
        este_lon = params_completos['right']
        
        # Crear polígono para toda el área
        puntos_area = [
            [norte_lat, oeste_lon],
            [norte_lat, este_lon],
            [sur_lat, este_lon],
            [sur_lat, oeste_lon]
        ]
        
        # Añadir el polígono del área completa al mapa
        folium.Polygon(
            locations=puntos_area,
            color='black',
            weight=3,
            fill=False,
            tooltip="Área completa de Santiago para scraping"
        ).add_to(m)
    except Exception as e:
        print(f"Error al dibujar el área completa: {e}")
    
    # Dibujar cada celda de la cuadrícula
    for i in range(total_celdas):
        try:
            # Generar parámetros para esta celda
            params = generar_parametros_santiago(i, total_celdas)
            
            # Obtener el color para esta celda (rotación de colores)
            color = colors[i % len(colors)]
            
            # Crear polígono para esta celda
            points = [
                [params['top'], params['left']],
                [params['top'], params['right']],
                [params['bottom'], params['right']],
                [params['bottom'], params['left']]
            ]
            
            # Añadir el polígono al mapa
            folium.Polygon(
                locations=points,
                color=color,
                weight=2,
                fill=True,
                fill_color=color,
                fill_opacity=0.2,
                tooltip=f"Celda {i+1}: {params['zona_aproximada']}"
            ).add_to(m)
            
            # Añadir un marcador con el número e información de la celda
            folium.Marker(
                location=[(params['top'] + params['bottom']) / 2, (params['left'] + params['right']) / 2],
                icon=folium.DivIcon(html=f'<div style="font-size: 12pt; font-weight: bold; color: {color};">{i+1}</div>'),
                tooltip=f"Celda {i+1}: {params['zona_aproximada']}"
            ).add_to(m)
        except Exception as e:
            print(f"Error al dibujar celda {i+1}: {e}")
    
    # Guardar el mapa en un archivo temporal y abrirlo
    with NamedTemporaryFile(delete=False, suffix='.html') as tmp:
        map_path = tmp.name
        m.save(map_path)
        
    # Abrir el mapa en el navegador
    webbrowser.open('file://' + map_path)
    print(f"Mapa abierto en el navegador. Archivo: {map_path}")
    
    return map_path

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
    
    # Visualizar la cuadrícula en un mapa antes de comenzar el scraping
    map_path = visualizar_cuadricula(total_celdas)
    
    # Preguntar al usuario si desea continuar después de ver el mapa
    respuesta = input("\n¿Desea continuar con el scraping usando esta cuadrícula? (s/n): ")
    if respuesta.lower() != 's':
        print("Operación cancelada por el usuario.")
        exit()
    
    # Create 'jsons' directory if it doesn't exist
    jsons_dir = 'jsons'
    if not os.path.exists(jsons_dir):
        os.makedirs(jsons_dir)
        print(f"Directorio creado: {jsons_dir}")
    
    # Timestamp for this batch of requests
    batch_timestamp = time.strftime("%Y%m%d-%H%M%S")
    
    # Estructura para almacenar todos los datos
    resultado_completo = {
        "timestamp": batch_timestamp,
        "fecha": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_celdas": total_celdas,
        "grid": f"{columnas}x{filas}",
        "region": "Santiago",
        "coordenadas_area": {
            "superior_izquierda": {"lat": -33.20000, "lon": -71.00000},
            "superior_derecha": {"lat": -33.20342, "lon": -70.50594},
            "inferior_izquierda": {"lat": -33.65306, "lon": -70.99022},
            "inferior_derecha": {"lat": -33.70686, "lon": -70.46603}
        },
        "alertas_totales": 0,
        "atascos_totales": 0,
        "usuarios_totales": 0,
        "celdas": []
    }
    
    # Generate parameters and fetch data for each cell in the grid
    for i in range(total_celdas):
        print(f"\nConsultando celda {i+1}/{total_celdas} (Cuadrícula {columnas}x{filas})...")
        
        # Generate parameters for this cell of the Santiago grid
        coords = generar_parametros_santiago(i, total_celdas)
        
        # Get Waze data for these coordinates
        data = get_waze_data(coords)
        
        # Almacenamos los datos en la estructura completa
        if data:
            # Contamos la cantidad de alertas, atascos y usuarios
            num_alertas = len(data.get('alerts', []))
            num_atascos = len(data.get('jams', []))
            num_usuarios = len(data.get('users', []))
            
            # Actualizamos los contadores totales
            resultado_completo["alertas_totales"] += num_alertas
            resultado_completo["atascos_totales"] += num_atascos
            resultado_completo["usuarios_totales"] += num_usuarios
            
            # Agregamos esta celda al resultado completo
            resultado_completo["celdas"].append({
                "metadata": data["location_metadata"],
                "data": data
            })
            
            print(f"Celda {i+1} procesada con éxito: {num_alertas} alertas, {num_atascos} atascos, {num_usuarios} usuarios")
        
        # Add a small delay between requests to avoid rate limiting
        if i < total_celdas - 1:
            time.sleep(2)  # Tiempo de espera entre peticiones
    
    # Guardamos todo en un solo archivo JSON
    filename = f"{jsons_dir}/waze_santiago_completo_{batch_timestamp}.json"
    with open(filename, 'w') as f:
        json.dump(resultado_completo, f, indent=4)
    
    print("\n=== SCRAPING COMPLETADO ===")
    print(f"Se consultaron {total_celdas} celdas en Santiago en una cuadrícula {columnas}x{filas}")
    print(f"Resultados consolidados guardados en: {filename}")
    print(f"Total de alertas recopiladas: {resultado_completo['alertas_totales']}")
    print(f"Total de atascos recopilados: {resultado_completo['atascos_totales']}")
    print(f"Total de usuarios detectados: {resultado_completo['usuarios_totales']}")
    
    # Abrir nuevamente el mapa para referencia
    print(f"\nPara volver a ver el mapa de cuadrículas, abra: {map_path}")