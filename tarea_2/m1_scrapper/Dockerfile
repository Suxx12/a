#Trae la imagen de python de docker hub
FROM python:3.13

#Setea el directorio de trabajo, crea la carpeta app y dice que aqui se va a trabajar
WORKDIR /app

#Copia el archivo requirements.txt al contenedor, este se usa para saber cuales son las librerias que usan los scripts
COPY requirements.txt ./

#Instala las librerias
RUN pip install --no-cache-dir -r requirements.txt

#Copia el resto de archivos que faltan (aca copia los scripts y el resto de cosas)
COPY . .

#Crea un volumen para los datos persistentes, este volumen es gestionado por docker y no se puede acceder directamente desde el host cuando el contenedor no esta corriendo
#VOLUME /app/jsons

#Ejecuta el script del scrapper
CMD [ "python", "scrapper.py" ]
#CMD ["ls"]

#No es necesario exponer ningun puerto ya que no se va a acceder al scrapper desde afuera