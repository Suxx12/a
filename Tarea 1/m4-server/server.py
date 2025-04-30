from flask import Flask, request, jsonify
import redis
import pymongo
import json
import os
from bson import json_util
import time

app = Flask(__name__)

# Conexión a Redis
redis_client = redis.Redis(
    host=os.environ.get('REDIS_HOST', 'localhost'),
    port=int(os.environ.get('REDIS_PORT', 6379)),
    db=0
)

# Conexión a MongoDB con autenticación
mongo_uri = os.environ.get('MONGO_URI', 'mongodb://localhost:27017/')
mongo_client = pymongo.MongoClient(mongo_uri)
mongo_db = mongo_client[os.environ.get('MONGO_DB', 'trafico_rm')]

# Acceso a las colecciones
alertas_collection = mongo_db['alertas']
atascos_collection = mongo_db['atascos']

# Tiempo de expiración de caché en Redis (en segundos)
CACHE_EXPIRATION = 3600  # 1 hora
# Agrega esto justo antes del manejador de errores

@app.route('/favicon.ico')
def favicon():
    return '', 204  # Devuelve una respuesta vacía con código 204 (No Content)

@app.route('/', methods=['GET'])
def index():
    return '''
        <h1>Bienvenido a la API de tráfico</h1>
        <img src="https://i.imgur.com/nkoz0qO.jpeg" width="800">
    '''

# Rutas para obtener todos los UUIDs de alertas y atascos
@app.route('/uuids_alertas', methods=['GET'])
def get_uuids_alertas():
    try:
        # Proyección para obtener solo los UUIDs de las alertas
        uuids = list(alertas_collection.find({}, {"uuid": 1, "_id": 0}))
        
        # Extraer solo los valores de UUID en una lista simple
        uuid_list = [doc.get('uuid') for doc in uuids if doc.get('uuid')]
        
        app.logger.info(f"Retrieved {len(uuid_list)} alertas UUIDs from MongoDB")
        return jsonify(uuid_list)
    
    except Exception as e:
        app.logger.error(f"Error retrieving alertas UUIDs: {str(e)}")
        return jsonify({'error': f'Error retrieving alertas UUIDs: {str(e)}'}), 500

# Reemplazar la función get_uuids_atascos existente

@app.route('/uuids_atascos', methods=['GET'])
def get_uuids_atascos():
    print("Entrando a la ruta /uuids_atascos")
    app.logger.info("Entrando a la ruta /uuids_atascos")
    try:
        # Verificar conexión
        server_info = mongo_client.server_info()
        app.logger.info(f"Conexión a MongoDB exitosa: versión {server_info.get('version', 'desconocida')}")
        
        # Contar documentos antes de la consulta
        total_docs = atascos_collection.count_documents({})
        app.logger.info(f"Total de documentos en la colección atascos: {total_docs}")
        
        # Proyección para obtener solo los UUIDs de los atascos
        uuids = list(atascos_collection.find({}, {"uuid": 1, "_id": 0}))
        app.logger.info(f"Documentos recuperados en la proyección: {len(uuids)}")
        
        # Diagnóstico: Inspeccionar documentos recuperados
        for i, doc in enumerate(uuids[:5]):  # Solo los primeros 5 para no saturar logs
            app.logger.info(f"Documento {i}: {doc}")
        
        # Extraer solo los valores de UUID en una lista simple
        uuid_list = [doc.get('uuid') for doc in uuids if doc.get('uuid')]
        
        app.logger.info(f"UUIDs extraídos: {len(uuid_list)}")
        if uuid_list:
            app.logger.info(f"Primeros UUIDs: {uuid_list[:5]}")
        else:
            app.logger.warning("No se encontraron UUIDs en los documentos")
        
        return jsonify(uuid_list)
        
    except pymongo.errors.ConnectionFailure as e:
        app.logger.error(f"Error de conexión a MongoDB: {str(e)}")
        return jsonify({'error': f'Error de conexión a MongoDB: {str(e)}'}), 500
        
    except Exception as e:
        app.logger.error(f"Error retrieving atascos UUIDs: {str(e)}")
        return jsonify({'error': f'Error retrieving atascos UUIDs: {str(e)}'}), 500
# FIN



# Agregar después de la ruta /uuids_atascos

@app.route('/diagnostico_atascos', methods=['GET'])
def diagnostico_atascos():
    try:
        # Verificar conexión a MongoDB
        server_info = mongo_client.server_info()
        
        # Obtener lista de bases de datos
        databases = mongo_client.list_database_names()
        
        # Obtener lista de colecciones en la base de datos actual
        collections = mongo_db.list_collection_names()
        
        # Contar documentos en la colección de atascos
        count_atascos = atascos_collection.count_documents({})
        
        # Obtener muestra de documentos (máximo 5)
        sample_docs = list(atascos_collection.find().limit(5))
        sample_docs_json = json.loads(json_util.dumps(sample_docs))
        
        # Contar documentos con campo UUID
        docs_with_uuid = atascos_collection.count_documents({"uuid": {"$exists": True}})
        
        # Extraer tipos de datos usados en el campo UUID (para detectar mezclas de tipos)
        pipeline = [
            {"$match": {"uuid": {"$exists": True}}},
            {"$project": {"uuid_type": {"$type": "$uuid"}}},
            {"$group": {"_id": "$uuid_type", "count": {"$sum": 1}}}
        ]
        uuid_types = list(atascos_collection.aggregate(pipeline))
        uuid_types_json = json.loads(json_util.dumps(uuid_types))
        
        return jsonify({
            "estado_conexion": "exitosa",
            "version_mongodb": server_info.get("version", "desconocida"),
            "bases_de_datos": databases,
            "colecciones": collections,
            "total_documentos_atascos": count_atascos,
            "documentos_con_uuid": docs_with_uuid,
            "tipos_de_uuid": uuid_types_json,
            "muestra_documentos": sample_docs_json
        })
    except Exception as e:
        app.logger.error(f"Error en diagnóstico: {str(e)}")
        return jsonify({
            "estado_conexion": "fallida",
            "error": str(e)
        }), 500



#Rutas para que el generador de trafico pregunte por alertas y atascos

@app.route('/alerta/<uuid>', methods=['GET'])
def get_alerta(uuid):
    # Buscar en Redis primero
    cache_key = f"alerta:{uuid}"
    inicio_tiempo_cache = time.time()
    cached_data = redis_client.get(cache_key)
    fin_tiempo_cache = time.time()
    tiempo_cache = fin_tiempo_cache - inicio_tiempo_cache
    tiempo_cache_ms = round(tiempo_cache * 1000, 2)  # Convertir a milisegundos
    
    if cached_data:
        # Si el registro está en caché
        app.logger.info(f"Cache hit for alerta UUID: {uuid}")
        return jsonify({"resultado": "hit", "tiempo (ms)": tiempo_cache_ms})
    
    # Si no está en Redis, buscar en MongoDB
    app.logger.info(f"Cache miss for alerta UUID: {uuid}, querying MongoDB")

    inicio_tiempo_mongo = time.time()
    alerta = alertas_collection.find_one({"uuid": uuid})
    fin_tiempo_mongo = time.time()
    tiempo_mongo = fin_tiempo_mongo - inicio_tiempo_mongo
    tiempo_mongo_ms = round(tiempo_mongo * 1000, 2)  # Convertir a milisegundos
    if not alerta:
        return jsonify({"resultado": "no_encontrado"})
    
    # Convertir el documento BSON a JSON
    alerta_json = json.loads(json_util.dumps(alerta))
    
    # Guardar en Redis para futuras consultas
    redis_client.setex(
        cache_key,
        CACHE_EXPIRATION,
        json.dumps(alerta_json)
    )
    
    return jsonify({"resultado": "miss", "tiempo (ms)": tiempo_mongo_ms})

@app.route('/atasco/<uuid>', methods=['GET'])
def get_atasco(uuid):
    # Buscar en Redis primero
    cache_key = f"atasco:{uuid}"
    cached_data = redis_client.get(cache_key)
    
    if cached_data:
        # Si el registro está en caché
        app.logger.info(f"Cache hit for atasco UUID: {uuid}")
        return jsonify({"resultado": "hit"})
    
    # Si no está en Redis, buscar en MongoDB
    app.logger.info(f"Cache miss for atasco UUID: {uuid}, querying MongoDB")
    
    # 1. Intentar buscar con el UUID como string
    atasco = atascos_collection.find_one({"uuid": uuid})
    
    # 2. Si no se encuentra y parece ser un número, intentar con formato numérico
    if not atasco and uuid.isdigit():
        atasco = atascos_collection.find_one({"uuid": int(uuid)}) # Se convierte a int porque los uuids de atasco son in
    
    # 3. Si sigue sin encontrarse, verificar si es un UUID con guiones
    if not atasco:
        # Intentar quitar los guiones por si está almacenado sin ellos
        uuid_sin_guiones = uuid.replace('-', '')
        atasco = atascos_collection.find_one({"uuid": uuid_sin_guiones})
    
    if not atasco:
        return jsonify({"resultado": "no_encontrado"})
    
    # Convertir el documento BSON a JSON
    atasco_json = json.loads(json_util.dumps(atasco))
    
    # Guardar en Redis para futuras consultas
    redis_client.setex(
        cache_key,
        CACHE_EXPIRATION,
        json.dumps(atasco_json)
    )
    
    return jsonify({"resultado": "miss"})
#FIN

#Rutas para buscar directo en mongo y comparar latencia
@app.route('/alerta_mongo/<uuid>', methods=['GET'])
def get_alerta_mongo(uuid):
    
    # Si no está en Redis, buscar en MongoDB
    app.logger.info(f"Cache miss for alerta UUID: {uuid}, querying MongoDB")
    alerta = alertas_collection.find_one({"uuid": uuid})
    
    if not alerta:
        return jsonify({"resultado": "no_encontrado"})
    
    
    return jsonify({"resultado": "miss"})

@app.route('/atasco_mongo/<uuid>', methods=['GET'])
def get_atasco_mongo(uuid):
    
    # Si no está en Redis, buscar en MongoDB
    app.logger.info(f"Cache miss for atasco UUID: {uuid}, querying MongoDB")
    
    # 1. Intentar buscar con el UUID como string
    atasco = atascos_collection.find_one({"uuid": uuid})
    
    # 2. Si no se encuentra y parece ser un número, intentar con formato numérico
    if not atasco and uuid.isdigit():
        atasco = atascos_collection.find_one({"uuid": int(uuid)}) # Se convierte a int porque los uuids de atasco son in
    
    # 3. Si sigue sin encontrarse, verificar si es un UUID con guiones
    if not atasco:
        # Intentar quitar los guiones por si está almacenado sin ellos
        uuid_sin_guiones = uuid.replace('-', '')
        atasco = atascos_collection.find_one({"uuid": uuid_sin_guiones})
    
    if not atasco:
        return jsonify({"resultado": "no_encontrado"})
    
    
    return jsonify({"resultado": "miss"})



@app.errorhandler(Exception)
def handle_error(e):
    app.logger.error(f"Error: {str(e)}")
    return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))