# Proyecto Kafka-Debezium-Zookeeper

Este proyecto demuestra una configuración básica para utilizar Docker, Kafka, Zookeeper, Debezium, Postgres y Mongodb  para la transmisión de cambios en una base 
de datos PostgreSQL a un topic de Kafka. Además, se proporciona un script de Python (consumer.py) que consume y guarda en mongo db atlas.

## Instrucciones de Uso

### 1. Ejecutar el Contenedor

Asegúrate de tener Docker y Docker Compose instalados en tu máquina. Luego, ejecuta el siguiente comando en la raíz del proyecto:

sudo docker-compose up --build

### 2. Cargar la Tabla de Prueba

En el archivo data.txt se encuentra un tabla de pruebas para que debezium monitoree. se le debe cargar a postgres, las credenciales
se encuentra en docker-compose.yml

### 3. Crear conector con Debezium

A traves de POSTMAN se envia a http://localhost:8083/connectors un metodo POST y genera un conector, se puede observar en localhost:8081 o localhost:8083/connectors
{
  "name": "users-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
     "database.port": "5432",
     "database.user": "postgres",
     "database.password": "postgres",
     "database.dbname" : "postgres",
     "database.server.name": "dbserver1",
     "schema.include.list": "public",
     "table.include.list": "public.users",
     "topic.prefix": "dbserver"
    //  "slot.name": "soltid_1"
  }
}

IMPORTANTE: Si modificamos algun campo aqui es importante hacerlo en el script consumer.py y en nuestra configuracion de postgres

### 4. Ingreso a mongodbatlas

Debemos ingresar y habilitar que nos permitar hacer insercciones desde nuestra red, luego ya podemos ingresar al cluster y ver nuestra base de datos y sus
colecciones
Email: mongotestjudicial@gmail.com
Password: Hola1234!

### 5. Verificar connection a mongo

Verificar coneccion a mongodb, para ello correr el script testdb.py

### 6. Verificar funcionamiento

Probar que todo funcione correctemente y se nos este monitoreando los cambios y enviando a mongo
Este README es un ejemplo básico y puedes personalizarlo según las necesidades específicas de tu proyecto.
