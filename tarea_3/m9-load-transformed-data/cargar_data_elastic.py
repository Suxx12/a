from elasticsearch import Elasticsearch

es = Elasticsearch("http://elasticsearch:9200")

if not es.ping():
    raise Exception("No se pudo conectar a Elasticsearch")

doc = {
    "nombre": "Camilo",
    "mensaje": "¡Hola desde Python en Docker!"
}

res = es.index(index="pruebas", document=doc)
print("Resultado:", res['result'])
