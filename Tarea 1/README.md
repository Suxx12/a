# Proyecto de sistemas distribuidos
Nombres: Renato Yañez, Camilo Rios \
Sección: 03 \
Profesor: Nicolás Hidalgo
## Levantamiento del Proyecto

### Alternativa 1: Levantar todos los servicios con un único `docker-compose`

Esta alternativa permite iniciar todos los módulos del proyecto de forma centralizada a través de un único archivo `docker-compose`. Al ejecutar este archivo principal, se encarga automáticamente de construir y levantar los servicios definidos para cada módulo del sistema.

Para iniciar el proyecto, desde el directorio `TAREA 1`, ejecuta el siguiente comando:

```bash
docker compose up --build -d
```

>[!NOTE]
>Esto levantará el contenedor de redis con la politica de remocion LRU y 1MB de memoria máxima, además del contenedor del generador de tráfico utilizando la distribucion zipf para repeticion de consultas y poisson para las tasas de arribo. 

### Interfaces disponibles

Una vez levantados los servicios, estarán disponibles las siguientes interfaces web:

- **Mongo Express**: [http://localhost:8081](http://localhost:8081)
- **Redis Insight**: [http://localhost:8001](http://localhost:8001)
- **Servidor Web (API)**: [http://localhost:5000](http://localhost:5000)



Para ver los logs individualmente de cada modulo:


```bash
#Scrapper
docker logs waze-scrapper-general

# Base de datos
docker logs mongodb-general
docker logs mongo-express-general

# Redis y su interfaz
docker logs redis_general
docker logs redis_insight_general

# Backend y procesamiento
docker logs api_server_general
docker logs json-loader-general
docker logs generador_trafico_normal_cache_general
```

### Alternativa 2: Levantar los modulos individualmente.

Para levantar los modulos individualmente, simplemente se accede al directorio del modulo con:
```bash
cd "mn_nombre-del-modulo"
```
y se ejecuta el docker-compose con:
```bash
docker compose up --build -d
```

⚠️ **Importante:** Los módulos tienen que levantarse en el siguiente orden, debido a las dependencias entre ellos:

1. `m2_almacenamiento`
2. `m3-cache`
3. `m0_cargajson`
4. `m1_scrapper`
5. `m4-server`
6. `m5-generador-trafico (Normal o Zipf)` 


Y finalmente los modulos de visualización de datos.
