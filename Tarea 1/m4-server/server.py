from flask import Flask, request, jsonify
import redis
import pymongo
import json
import os
from bson import json_util

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
mongo_db = mongo_client[os.environ.get('MONGO_DB', 'trafficdb')]

# Acceso a las colecciones
alertas_collection = mongo_db['alertas']
atascos_collection = mongo_db['atascos']

# Tiempo de expiración de caché en Redis (en segundos)
CACHE_EXPIRATION = 3600  # 1 hora

@app.route('/', methods=['GET'])
def index():
    return '''
        <h1>Bienvenido a la API de tráfico</h1>
        <img src="https://i.imgur.com/nkoz0qO.jpeg" width="800">
    '''
@app.route('/alerta/<uuid>', methods=['GET'])
def get_alerta(uuid):
    # Buscar en Redis primero
    cache_key = f"alerta:{uuid}"
    cached_data = redis_client.get(cache_key)
    
    if cached_data:
        # Si el registro está en caché, devolver datos de Redis
        app.logger.info(f"Cache hit for alerta UUID: {uuid}")
        return jsonify(json.loads(cached_data))
    
    # Si no está en Redis, buscar en MongoDB
    app.logger.info(f"Cache miss for alerta UUID: {uuid}, querying MongoDB")
    alerta = alertas_collection.find_one({"uuid": uuid})
    
    if not alerta:
        return jsonify({'error': 'Alerta not found'}), 404
    
    # Convertir el documento BSON a JSON
    alerta_json = json.loads(json_util.dumps(alerta))
    
    # Guardar en Redis para futuras consultas
    redis_client.setex(
        cache_key,
        CACHE_EXPIRATION,
        json.dumps(alerta_json)
    )
    
    return jsonify(alerta_json)

@app.route('/atasco/<uuid>', methods=['GET'])
def get_atasco(uuid):
    # Buscar en Redis primero
    cache_key = f"atasco:{uuid}"
    cached_data = redis_client.get(cache_key)
    
    if cached_data:
        # Si el registro está en caché, devolver datos de Redis
        app.logger.info(f"Cache hit for atasco UUID: {uuid}")
        return jsonify(json.loads(cached_data))
    
    # Si no está en Redis, buscar en MongoDB
    app.logger.info(f"Cache miss for atasco UUID: {uuid}, querying MongoDB")
    
    # 1. Intentar buscar con el UUID como string
    atasco = atascos_collection.find_one({"uuid": uuid})
    
    # 2. Si no se encuentra y parece ser un número, intentar con formato numérico
    if not atasco and uuid.isdigit():
        atasco = atascos_collection.find_one({"uuid": int(uuid)})
    
    # 3. Si sigue sin encontrarse, verificar si es un UUID con guiones
    if not atasco:
        # Intentar quitar los guiones por si está almacenado sin ellos
        uuid_sin_guiones = uuid.replace('-', '')
        atasco = atascos_collection.find_one({"uuid": uuid_sin_guiones})
    
    if not atasco:
        return jsonify({'error': 'Atasco not found'}), 404
    
    # Convertir el documento BSON a JSON
    atasco_json = json.loads(json_util.dumps(atasco))
    
    # Guardar en Redis para futuras consultas
    redis_client.setex(
        cache_key,
        CACHE_EXPIRATION,
        json.dumps(atasco_json)
    )
    
    return jsonify(atasco_json)


@app.route('/atascos', methods=['GET'])
def get_all_atascos():
    # Verificar si tenemos la lista completa en caché
    cache_key = "all_atascos"
    cached_data = redis_client.get(cache_key)
    
    if cached_data:
        app.logger.info("Cache hit for all atascos")
        return jsonify(json.loads(cached_data))
    
    # Si no está en caché, consultar MongoDB
    app.logger.info("Cache miss for all atascos, querying MongoDB")
    
    try:
        # Obtener todos los atascos
        atascos = list(atascos_collection.find())
        
        if not atascos:
            return jsonify({'message': 'No atascos found', 'data': []}), 200
        
        # Convertir documentos BSON a JSON
        atascos_json = json.loads(json_util.dumps(atascos))
        
        # Guardar en caché para futuras consultas
        redis_client.setex(
            cache_key,
            CACHE_EXPIRATION,
            json.dumps(atascos_json)
        )
        
        return jsonify({'message': 'Atascos retrieved successfully', 'count': len(atascos_json), 'data': atascos_json})
    
    except Exception as e:
        app.logger.error(f"Error retrieving atascos: {str(e)}")
        return jsonify({'error': f'Error retrieving atascos: {str(e)}'}), 500


@app.route('/buscar', methods=['POST'])
def buscar_por_uuid():
    # Obtener UUID de la solicitud
    data = request.get_json()
    
    if not data or 'uuid' not in data:
        return jsonify({'error': 'UUID is required'}), 400
    
    uuid = data['uuid']
    
    # Intentar buscar en ambas colecciones
    result = {}
    
    # Verificar en caché primero
    alerta_cached = redis_client.get(f"alerta:{uuid}")
    atasco_cached = redis_client.get(f"atasco:{uuid}")
    
    # Procesar alerta
    if alerta_cached:
        result['alerta'] = json.loads(alerta_cached)
    else:
        alerta = alertas_collection.find_one({"uuid": uuid})
        if alerta:
            alerta_json = json.loads(json_util.dumps(alerta))
            result['alerta'] = alerta_json
            # Guardar en caché
            redis_client.setex(f"alerta:{uuid}", CACHE_EXPIRATION, json.dumps(alerta_json))
    
    # Procesar atasco
    if atasco_cached:
        result['atasco'] = json.loads(atasco_cached)
    else:
        atasco = atascos_collection.find_one({"uuid": uuid})
        if atasco:
            atasco_json = json.loads(json_util.dumps(atasco))
            result['atasco'] = atasco_json
            # Guardar en caché
            redis_client.setex(f"atasco:{uuid}", CACHE_EXPIRATION, json.dumps(atasco_json))
    
    if not result:
        return jsonify({'error': f'No se encontraron registros con UUID: {uuid}'}), 404
    
    return jsonify(result)

@app.errorhandler(Exception)
def handle_error(e):
    app.logger.error(f"Error: {str(e)}")
    return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))