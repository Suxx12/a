#!/usr/bin/env python3
import os
import json
import pymongo
from datetime import datetime

print("Iniciando exportación de datos de MongoDB...")

# Configuración desde variables de entorno
mongo_uri = os.environ.get('MONGODB_URI')
mongo_db = os.environ.get('MONGODB_DB')
alertas_collection = os.environ.get('MONGODB_COLECCION_ALERTAS')
atascos_collection = os.environ.get('MONGODB_COLECCION_ATASCOS')

print(f"Conectando a: {mongo_uri}, Base de datos: {mongo_db}")

# Conectar a MongoDB
client = pymongo.MongoClient(mongo_uri)
db = client[mongo_db]

# Timestamp para archivos únicos
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_dir = "/app/data"

# Exportar colección de alertas
print(f"Exportando colección: {alertas_collection}")
alertas_file = f"{output_dir}/alertas_{timestamp}.json"

# Crear un diccionario de objetos con IDs únicos
alertas_dict = {}
alertas_count = 0

for doc in db[alertas_collection].find({}, {'_id': 0}):
    # Usar un campo único como clave, o generar uno si no existe
    key = doc.get('uuid', f'item_{alertas_count}')
    alertas_dict[key] = doc
    alertas_count += 1

# Escribir como un objeto JSON
with open(alertas_file, 'w') as f:
    json.dump(alertas_dict, f, indent=2)

print(f"Exportadas {alertas_count} alertas a {alertas_file}")

# Exportar colección de atascos
print(f"Exportando colección: {atascos_collection}")
atascos_file = f"{output_dir}/atascos_{timestamp}.json"

# Crear un diccionario de objetos con IDs únicos
atascos_dict = {}
atascos_count = 0

for doc in db[atascos_collection].find({}, {'_id': 0}):
    # Usar un campo único como clave, o generar uno si no existe
    key = doc.get('uuid', f'item_{atascos_count}')
    atascos_dict[key] = doc
    atascos_count += 1

# Escribir como un objeto JSON
with open(atascos_file, 'w') as f:
    json.dump(atascos_dict, f, indent=2)

print(f"Exportados {atascos_count} atascos a {atascos_file}")

# Verificar que los archivos JSON sean válidos
print("Verificando integridad de los archivos JSON...")

def verificar_json(archivo):
    try:
        with open(archivo, 'r') as f:
            json.load(f)
        return True
    except json.JSONDecodeError as e:
        print(f"Error en archivo {archivo}: {e}")
        return False

if verificar_json(alertas_file) and verificar_json(atascos_file):
    print("Verificación exitosa: los archivos JSON son válidos.")
else:
    print("Se encontraron errores en los archivos JSON.")

print("Exportación de datos completada.")
print("Los archivos se encuentran en el directorio compartido '/app/data'")