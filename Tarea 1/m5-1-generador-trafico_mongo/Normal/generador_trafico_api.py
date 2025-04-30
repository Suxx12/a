import json
from scipy.stats import norm
import numpy as np
import requests
import time

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

# Generar distribución Normal
total_samples = 50000  # Cantidad de elementos en la distribución final
n = len(combined_uuids)

# Configurar la distribución normal
mu = n / 2  # Media en el centro de la lista
sigma = n / 5  # Ajustamos la desviación estándar para mejor distribución

print(f"Usando: mu={mu}, sigma={sigma}")

# Mezclar aleatoriamente los UUIDs para evitar sesgos por orden inicial
np.random.shuffle(combined_uuids)

# Usar scipy.stats.norm para crear la distribución
distribution = norm(loc=mu, scale=sigma)
# Generar valores aleatorios siguiendo la distribución
normal_values = distribution.rvs(size=total_samples)
# Redondear y limitar a los índices válidos
normal_indices = np.clip(np.round(normal_values).astype(int), 0, n-1)

# Crear una lista con la distribución normal
normal_uuids = [combined_uuids[idx] for idx in normal_indices]

# Opcional: Visualizar la distribución (descomentar si deseas verificar)
import matplotlib.pyplot as plt
plt.hist(normal_indices, bins=50)
plt.savefig('/app/distribucion_normal.png')
print("Se guardó una visualización de la distribución en /app/distribucion_normal.png")

# Guardar la distribución normal como JSON
with open("/app/uuids_normal_api.json", 'w') as f:
    json.dump(normal_uuids, f)

print(f"Se guardaron {len(normal_uuids)} UUIDs con distribución Normal en /app/uuids_normal_api.json")

# También guardar la lista original de UUIDs únicos
with open("/app/uuids_api.json", 'w') as f:
    json.dump(combined_uuids, f)

print(f"Se guardaron {len(combined_uuids)} UUIDs únicos en /app/uuids_api.json")

# Consultar la API para cada UUID en la distribución Normal
print("\nConsultando la API para cada UUID en la distribución Normal...")

# Contadores para estadísticas
total_requests = 0
cache_hits = 0
cache_misses = 0
not_found = 0

# Iniciar conteo de tiempo
start_time = time.time()

# Realizar las consultas a la API
for item in normal_uuids:
    tipo = item["tipo"]
    uuid = item["uuid"]
    
    # Construir la URL según el tipo
    if tipo == "alerta":
        url = f"{api_base_url}/alerta_mongo/{uuid}"
    elif tipo == "atasco":
        url = f"{api_base_url}/atasco_mongo/{uuid}"
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

# Calcular tiempo total de todas las consultas
end_time = time.time()
total_time = end_time - start_time

# Calcular el hit rate
hit_rate = (cache_hits / total_requests) * 100 if total_requests > 0 else 0

# Mostrar estadísticas
print("\nResultados de las consultas:")
print(f"Total de consultas: {total_requests}")
print(f"Cache hits: {cache_hits}")
print(f"Cache misses: {cache_misses}")
print(f"No encontrados: {not_found}")
print(f"Hit rate: {hit_rate:.2f}%")
print(f"Tiempo total: {total_time:.2f} segundos")

# Guardar los resultados en un archivo
results = {
    "total_requests": total_requests,
    "cache_hits": cache_hits,
    "cache_misses": cache_misses,
    "not_found": not_found,
    "hit_rate": hit_rate,
    "total_time_seconds": total_time,
    "average_time_per_request": (total_time / total_requests) if total_requests > 0 else 0
}

with open("/app/hit_rate_results.json", 'w') as f:
    json.dump(results, f)

print(f"Resultados guardados en /app/hit_rate_results.json")