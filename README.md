# Proyecto de sistemas distribuidos
Nombres: Renato Yañez, Camilo Rios \
Sección: 03 \
Profesor: Nicolás Hidalgo
## Levantamiento del Proyecto

### Alternativa 1: Levantar todos los servicios con un único `docker-compose`

Esta alternativa permite iniciar todos los módulos del proyecto de forma centralizada a través de un único archivo `docker-compose`. Al ejecutar este archivo principal, se encarga automáticamente de construir y levantar los servicios definidos para cada módulo del sistema.

Para iniciar el proyecto, desde el directorio `tarea_2`, ejecuta el siguiente comando:

```bash
docker compose up --build -d
```
### Interfaces disponibles

Una vez levantados los servicios, estarán disponibles las siguientes interfaces web:

- **Mongo Express**: [http://localhost:8081](http://localhost:8081)



Para ver los logs individualmente de cada modulo:


```bash
#Scrapper
docker logs waze-scrapper-general

# Base de datos
docker logs mongodb-general
docker logs mongo-express-general

# Filtering
docker logs filtering-general

# Processing
docker logs processing-general

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
2. `m1_scrapper`
3. `m7-filtering`
4. `m8-processing`.
