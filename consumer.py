import json
from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient

def initialize_mongo_connection(collection_name):
    mongo_client = MongoClient('mongodb+srv://root:root@cluster.esryp20.mongodb.net/?retryWrites=true&w=majority')
    mongo_db = mongo_client['mongodb']
    mongo_collection = mongo_db[collection_name]
    return mongo_collection

def process_event(event_data, mongo_collection):
    print("new message: :)")
    if event_data['payload']['op'] == 'c':
        document = event_data['payload']['after']
        mongo_collection.insert_one(document)
    elif event_data['payload']['op'] == 'u':
        document_id = event_data['payload']['after']['id']
        update_data = event_data['payload']['after']
        mongo_collection.update_one({'id': document_id}, {'$set': update_data})

def main():
    # Configuración de Kafka
    consumer_conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'thisgroup',
        'auto.offset.reset': 'earliest'
    }

    # Lista de colecciones y sus respectivos temas de Kafka
    collections = {
        'users': 'dbserver1.public.users',
        #'autos': 'dbserver1.public.autos'
        # Agrega más colecciones según sea necesario
    }

    # Inicializa consumidores de Kafka y conexiones de MongoDB
    consumers = {}
    mongo_collections = {}
    for collection_name, kafka_topic in collections.items():
        consumer = Consumer(consumer_conf)
        consumer.subscribe([kafka_topic])
        consumers[collection_name] = consumer
        mongo_collections[collection_name] = initialize_mongo_connection(collection_name)

    print("Start consumer mongo collections")

    try:
        while True:
            for collection_name, consumer in consumers.items():
                msg = consumer.poll(1.0)

                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(msg.error())
                        break

                event_data = json.loads(msg.value())
                process_event(event_data, mongo_collections[collection_name])

    except KeyboardInterrupt:
        pass
    finally:
        for consumer in consumers.values():
            consumer.close()

if __name__ == "__main__":
    main()
