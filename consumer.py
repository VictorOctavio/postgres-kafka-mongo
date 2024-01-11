import json
from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient

# Configuración del consumidor Kafka
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',  # Cambia esto si el broker de Kafka está en otro lugar
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_conf)

# mongodb+srv://root:<password>@cluster0.9baupcr.mongodb.net/

# Configuración de conexión a MongoDB
mongo_client = MongoClient('mongodb+srv://root:root@cluster0.9baupcr.mongodb.net/')  # Cambia esto si MongoDB está en otro lugar
mongo_db = mongo_client['mongodb']
mongo_collection = mongo_db['mongocollection']

# Suscripción al tema de Kafka
consumer.subscribe(['dbserver1.public.users'])


print("Start consumer mongo collection")

# Función para procesar eventos y actualizar MongoDB
def process_event(event_data, mongo_collection):
    
    print("new message: :)")
    # Implementa la lógica de procesamiento aquí
    # Utiliza la información del evento para realizar operaciones en MongoDB

    # Ejemplo: Insertar en MongoDB
    if event_data['payload']['op'] == 'c':  # 'c' indica una operación de inserción
        document = event_data['payload']['after']
        mongo_collection.insert_one(document)

    # Ejemplo: Actualizar en MongoDB
    elif event_data['payload']['op'] == 'u':  # 'u' indica una operación de actualización
        document_id = event_data['payload']['after']['_id']
        update_data = event_data['payload']['after']
        mongo_collection.update_one({'_id': document_id}, {'$set': update_data})

    # Puedes agregar lógica para otras operaciones (eliminar, por ejemplo) según sea necesario


# Procesamiento de eventos
try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break

        # Procesar el mensaje
        event_data = json.loads(msg.value())
        process_event(event_data, mongo_collection)

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
