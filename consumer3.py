import json
import datetime
import base64
from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient

# Initialize consumer kafka
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'thisgroup',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_conf)

# Initialize mongodb connection
mongo_client = MongoClient('mongodb+srv://root:root@cluster.esryp20.mongodb.net/?retryWrites=true&w=majority')
mongo_db = mongo_client['mongodb']
mongo_collection = mongo_db['accounts']

# Dictionary to store user states
user_states = {}

# Subscribe to kafka topics
consumer.subscribe(['dbserver1.bank.users', 'dbserver1.bank.accounts', 'dbserver1.bank.infouser'])


def decode_timestamp(encoded_timestamp):
    # Convert the binary timestamp to a datetime object
    return datetime.datetime.fromtimestamp(float(encoded_timestamp) / 1000000)

def decode_decimal(encoded_decimal):
    # Decode the base64 encoded decimal value
    decoded_bytes = base64.b64decode(encoded_decimal)
    # Convert the bytes to a float
    return float(int.from_bytes(decoded_bytes, byteorder='big'))



print("Start consumer mongo collection")

# Function to process events and update MongoDB
def process_event(event_data, mongo_collection):
    # Extract the source schema and table from the event data
    # source_schema = event_data['schema']['fields'][2]['fields'][8]['field']
    source_table = event_data['payload']['source']['table']

    # Check if it's a user creation event
    if source_table == 'users' and event_data['payload']['op'] == 'c': # 'c' indicates an insertion operation
        print("new message user insert: :)")
        # Store the user data in the state dictionary
        user_data = event_data['payload']['after']
        user_states[user_data['id']] = {
            'name': user_data['name'],
            'lastname': user_data['lastname'],
            'createdAt': decode_timestamp(user_data['createdat']).isoformat()
        }
        
    if source_table == 'infouser' and event_data['payload']['op'] == 'c': # 'c' indicates an insertion operation
        print("new message user insert: :)")
        # Store the user data in the state dictionary
        user_infodata = event_data['payload']['after']
        user_id = user_infodata['user_id']
        
        if user_id in user_states:
            user_states[user_id] = {
                **user_states[user_id],
                'phone': user_infodata['phone'],
                'address': user_infodata['address'],
                'country': user_infodata['country'],
                'city': user_infodata['city']
            }
        
    # Check if it's an account creation event
    elif source_table == 'accounts' and event_data['payload']['op'] == 'c':
        print("new message account insert: :)")
        # Retrieve the user data from the state dictionary
        account_data = event_data['payload']['after']
        user_id = account_data['user_id']
        if user_id in user_states:
            # Combine user data with account data
            document = {
                **user_states[user_id],
                'amount': decode_decimal(account_data['amount']),
            }
            # Insert the document into the MongoDB collection
            mongo_collection.insert_one(document)
            print("insert into MongoDB: ", document)
            # Clear hte state for this user since we have processed their account
            del user_states[user_id]

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
