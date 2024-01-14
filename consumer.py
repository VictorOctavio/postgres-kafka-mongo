import json
from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient

# Iinitialize consumer kafka
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'thisgroup',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_conf)

# Initialize mongodb connection
mongo_client = MongoClient('mongodb+srv://root:root@cluster.esryp20.mongodb.net/?retryWrites=true&w=majority')
mongo_db = mongo_client['mongodb']
mongo_collection = mongo_db['users']

# Subscribe to kafka topic
consumer.subscribe(['dbserver1.public.users'])

print("Start consumer mongo collection")

# Función para procesar eventos y actualizar MongoDB
def process_event(event_data, mongo_collection):
    print("new message: :)")
    # Implementa la lógica de procesamiento aquí

    # Insert a new user into the collection
    if event_data['payload']['op'] == 'c':  # 'c' indica una operación de inserción
        document = event_data['payload']['after']
        mongo_collection.insert_one(document)

    # Update User in MongoDB
    elif event_data['payload']['op'] == 'u':
        document_id = event_data['payload']['after']['id']
        update_data = event_data['payload']['after']
        mongo_collection.update_one({'id': document_id}, {'$set': update_data})


# Process events
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

        # Proces message
        event_data = json.loads(msg.value())
        process_event(event_data, mongo_collection)

except KeyboardInterrupt:
    pass
finally:
    consumer.close()