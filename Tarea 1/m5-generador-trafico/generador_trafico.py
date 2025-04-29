from pymongo import MongoClient
import json
from scipy.stats import zipf
import numpy as np

# Conexión a MongoDB
mongo_client = MongoClient(
    host="mongo",  # Nombre del contenedor
    port=27017,
    username="admin",
    password="admin123",
    authSource="admin"
)

# Seleccionar la base de datos
db = mongo_client["trafico_rm"]

# Lista para almacenar todos los UUIDs con su tipo
combined_uuids = []

# Obtener UUIDs de alertas
alertas = list(db["alertas"].find({}, {"_id": 0, "uuid": 1}))
for doc in alertas:
    combined_uuids.append({"tipo": "alerta", "uuid": doc["uuid"]})

# Obtener UUIDs de atascos
atascos = list(db["atascos"].find({}, {"_id": 0, "uuid": 1}))
for doc in atascos:
    combined_uuids.append({"tipo": "atasco", "uuid": doc["uuid"]})

print(f"Se encontraron {len(combined_uuids)} UUIDs únicos")

# Generar distribución Zipf
total_samples = 10000  # Cantidad de elementos en la distribución final
a = 1.1  # Parámetro 'a' de Zipf, mayor valor = más sesgo

# Generar índices siguiendo una distribución Zipf
n = len(combined_uuids)
zipf_indices = zipf.rvs(a=a, size=total_samples) - 1  # -1 porque zipf.rvs genera valores desde 1
zipf_indices = zipf_indices % n  # Asegurarse que los índices estén dentro del rango

# Crear una lista con la distribución Zipf
zipf_uuids = [combined_uuids[int(idx)] for idx in zipf_indices]

# Guardar la distribución Zipf como JSON
with open("/app/uuids_zipf.json", 'w') as f:
    json.dump(zipf_uuids, f)

print(f"Se guardaron {len(zipf_uuids)} UUIDs con distribución Zipf en /app/uuids_zipf.json")

# También guardar la lista original de UUIDs únicos
with open("/app/uuids.json", 'w') as f:
    json.dump(combined_uuids, f)

print(f"Se guardaron {len(combined_uuids)} UUIDs únicos en /app/uuids.json")

# Cerrar la conexión
mongo_client.close()