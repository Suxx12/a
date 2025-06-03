#!/usr/bin/env python3
# filepath: /home/renato/proyectos_universidad/sistemas_distribuidos/tarea_2/m7-filtering/exportar_mongo.py
import os
import csv
import pymongo
from datetime import datetime

def procesar_ciudad(city): ## CASO MALLOCO. MALLOCO SALIA COMO "PEÑAFLOR, MALLOCO" Y LA COMA NOS ROMPIA EL CSV, POR LO QUE REEMPLAZAMOS LA , POR UN ; (MALLOCO;PEÑAFLOR)
    """Procesa el campo ciudad para formatear y reemplazar comas por punto y coma"""
    if city and isinstance(city, str):
        city = city.replace(',', ';')
        city_parts = city.split(';')
        city_parts = [part.strip().title() for part in city_parts]
        return ';'.join(city_parts)
    return city

def main():
    print("Iniciando exportación desde MongoDB...")
    
    # Configuración desde variables de entorno
    mongo_uri = os.environ.get('MONGODB_URI')
    mongo_db = os.environ.get('MONGODB_DB')
    alertas_collection = os.environ.get('MONGODB_COLECCION_ALERTAS')
    atascos_collection = os.environ.get('MONGODB_COLECCION_ATASCOS')
    
    # Conectar a MongoDB
    client = pymongo.MongoClient(mongo_uri)
    db = client[mongo_db]
    
    # Timestamp para archivos únicos 
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = "/app/data"
    os.makedirs(output_dir, exist_ok=True)
    
    # Exportar alertas
    alertas_file = f"{output_dir}/transformed_alerta_alertas_{timestamp}.csv"
    campos_alertas = [
        "uuid", "city", "municipalityUser", "type", 
        "street", "confidence", "location_x", "location_y", "fecha"
    ]
    
    with open(alertas_file, 'w', newline='') as f:
        writer = csv.DictWriter(
            f, fieldnames=campos_alertas, 
            delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL
        )
        writer.writeheader()
        alertas_count = 0
        
        for doc in db[alertas_collection].find({}, {'_id': 0}):
            city = procesar_ciudad(doc.get("city", ""))
            fila = {
                "uuid": doc.get("uuid", f"item_{alertas_count}"),
                "city": city,
                "municipalityUser": doc.get("reportByMunicipalityUser", ""),
                "type": doc.get("type", ""),
                "street": doc.get("street", ""),
                "confidence": doc.get("confidence", 0),
                "location_x": doc.get("location", {}).get("x", doc.get("x", 0)),
                "location_y": doc.get("location", {}).get("y", doc.get("y", 0)),
                "fecha": doc.get("fecha", "")
            }
            writer.writerow(fila)
            alertas_count += 1
    
    print(f"Exportadas {alertas_count} alertas")
    
    # Exportar atascos
    atascos_file = f"{output_dir}/transformed_atasco_atascos_{timestamp}.csv"
    campos_atascos = [
        "uuid", "severity", "country", "length", "endnode", "roadtype", 
        "speed", "street", "fecha", "region", "city"
    ]
    
    with open(atascos_file, 'w', newline='') as f:
        writer = csv.DictWriter(
            f, fieldnames=campos_atascos, 
            delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL
        )
        writer.writeheader()
        atascos_count = 0
        
        for doc in db[atascos_collection].find({}, {'_id': 0}):
            city = procesar_ciudad(doc.get("city", ""))
            fila = {
                "uuid": doc.get("uuid", f"item_{atascos_count}"),
                "severity": doc.get("severity", ""),
                "country": doc.get("country", ""),
                "length": doc.get("length", ""),
                "endnode": doc.get("endNode", ""),
                "roadtype": doc.get("roadType", ""),
                "speed": doc.get("speedKMH", doc.get("speed", "")),
                "street": doc.get("street", ""),
                "fecha": doc.get("fecha", ""),
                "region": doc.get("region", ""),
                "city": city
            }
            writer.writerow(fila)
            atascos_count += 1
    
    print(f"Exportados {atascos_count} atascos")
    print("Exportación completada.")

if __name__ == "__main__":
    main()