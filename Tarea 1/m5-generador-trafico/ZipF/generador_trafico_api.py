import json
from scipy.stats import zipf
import numpy as np
import requests

# Definir la URL base de la API
api_base_url = "http://api_server:5000"  # Usando el nombre del servicio como hostname

# Lista para almacenar todos los UUIDs con su tipo
combined_uuids = []

# Obtener UUIDs de alertas desde la API
try:
    alertas_response = requests.get(f"{api_base_url}/uuids_alertas")
    alertas_response.raise_for_status()  # Lanzar excepción si hay error HTTP
    alertas = alertas_response.json()
    
    for uuid in alertas:
        combined_uuids.append({"tipo": "alerta", "uuid": uuid})
    
    print(f"Se obtuvieron {len(alertas)} UUIDs de alertas desde la API")
except requests.exceptions.RequestException as e:
    print(f"Error al obtener UUIDs de alertas desde la API: {e}")
    alertas = []

# Obtener UUIDs de atascos desde la API
try:
    atascos_response = requests.get(f"{api_base_url}/uuids_atascos")
    atascos_response.raise_for_status()  # Lanzar excepción si hay error HTTP
    atascos = atascos_response.json()
    
    for uuid in atascos:
        combined_uuids.append({"tipo": "atasco", "uuid": uuid})
    
    print(f"Se obtuvieron {len(atascos)} UUIDs de atascos desde la API")
except requests.exceptions.RequestException as e:
    print(f"Error al obtener UUIDs de atascos desde la API: {e}")
    atascos = []

print(f"Se encontraron {len(combined_uuids)} UUIDs únicos en total")

# Si no se obtuvieron UUIDs, terminar
if not combined_uuids:
    print("No se obtuvieron UUIDs. Verificar la conexión con la API o si hay datos disponibles.")
    exit(1)

# Generar distribución Zipf
total_samples = 100000  # Cantidad de elementos en la distribución final
a = 1.3  # Parámetro 'a' de Zipf, mayor valor = más sesgo

# Generar índices siguiendo una distribución Zipf
n = len(combined_uuids)
zipf_indices = zipf.rvs(a=a, size=total_samples) - 1  # -1 porque zipf.rvs genera valores desde 1
zipf_indices = zipf_indices % n  # Asegurarse que los índices estén dentro del rango

# Crear una lista con la distribución Zipf
zipf_uuids = [combined_uuids[int(idx)] for idx in zipf_indices]

# Guardar la distribución Zipf como JSON
with open("/app/uuids_zipf_api.json", 'w') as f:
    json.dump(zipf_uuids, f)

print(f"Se guardaron {len(zipf_uuids)} UUIDs con distribución Zipf en /app/uuids_zipf.json")

# También guardar la lista original de UUIDs únicos
with open("/app/uuids_api.json", 'w') as f:
    json.dump(combined_uuids, f)



print(f"Se guardaron {len(combined_uuids)} UUIDs únicos en /app/uuids.json")

# Consultar la API para cada UUID en la distribución Zipf
print("\nConsultando la API para cada UUID en la distribución Zipf...")

# Contadores para estadísticas
total_requests = 0
cache_hits = 0
cache_misses = 0
not_found = 0

# Realizar las consultas a la API
for item in zipf_uuids:
    tipo = item["tipo"]
    uuid = item["uuid"]
    
    # Construir la URL según el tipo
    if tipo == "alerta":
        url = f"{api_base_url}/alerta/{uuid}"
    elif tipo == "atasco":
        url = f"{api_base_url}/atasco/{uuid}"
    else:
        print(f"Tipo desconocido: {tipo}")
        continue
    
    total_requests += 1
    
    try:
        response = requests.get(url)
        response.raise_for_status()  # Lanzar excepción si hay error HTTP
        
        # Analizar la respuesta para determinar si fue hit, miss o no_encontrado
        data = response.json()
        
        if data.get('resultado') == 'hit':
            cache_hits += 1
        elif data.get('resultado') == 'miss':
            cache_misses += 1
        elif data.get('resultado') == 'no_encontrado':
            not_found += 1
        
    except requests.exceptions.RequestException as e:
        print(f"Error al consultar {tipo} con UUID {uuid}: {e}")

# Calcular el hit rate
hit_rate = (cache_hits / total_requests) * 100 if total_requests > 0 else 0

# Mostrar estadísticas
print("\nResultados de las consultas:")
print(f"Total de consultas: {total_requests}")
print(f"Cache hits: {cache_hits}")
print(f"Cache misses: {cache_misses}")
print(f"No encontrados: {not_found}")
print(f"Hit rate: {hit_rate:.2f}%")

# Opcionalmente, guardar los resultados en un archivo
results = {
    "total_requests": total_requests,
    "cache_hits": cache_hits,
    "cache_misses": cache_misses,
    "not_found": not_found,
    "hit_rate": hit_rate
}

with open("/app/hit_rate_results.json", 'w') as f:
    json.dump(results, f)

print(f"Resultados guardados en /app/hit_rate_results.json")