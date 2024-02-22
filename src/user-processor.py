import logging
import sys
import json
import os
from pyflink.common import Types, Configuration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema

# Handler state
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.functions import MapFunction, KeyedProcessFunction, RuntimeContext


class EventToJson(MapFunction):
    def map(self, value):
        return json.loads(value) # Convertir el valor de cadena a un objeto JSON

class UserStateProcessFunction(KeyedProcessFunction):
    def __init__(self):
        self.state = None
        
    def open(self, runtime_context: RuntimeContext):
        self.state = runtime_context.get_state(ValueStateDescriptor(
            "user_states",  # the state name
            Types.STRING()  # type information
        ))

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        user_key = ctx.get_current_key()
        process_event(value, self.state, user_key)
        print(json.loads(self.state.value()))
        
def process_event(event_data, state, user_key):
    source_table = event_data['payload']['source']['table']
        
    if source_table == 'users' and event_data['payload']['op'] == 'c': # 'c' indicates an insertion operation
        user_data = event_data['payload']['after']
        current_state_value = state.value()
        if current_state_value is None:
            current_state_value = {}
        else:
            current_state_value = json.loads(current_state_value)
        current_state_value[user_key] = {
            'name': user_data['name'],
            'lastname': user_data['lastname'],
            'createdAt': user_data['createdat']
        }
        state.update(json.dumps(current_state_value))

    elif source_table == 'infouser' and event_data['payload']['op'] == 'c': # 'c' indicates an insertion operation
        user_infodata = event_data['payload']['after']
        user_id = user_infodata['user_id']
        current_state_value = state.value()
        if current_state_value is None:
            state.update(json.dumps({}))
            current_state_value = {}
        else:
            current_state_value = json.loads(current_state_value)
        current_state_value[user_id] = {
            'phone': user_infodata['phone'],
            'address': user_infodata['address'],
            'country': user_infodata['country'],
            'city': user_infodata['city']
        }
        state.update(json.dumps(current_state_value))
    
def read_from_kafka(env):
    
    properties = {
        "bootstrap.servers": "172.19.0.6:9092",
        "group.id": "thisgroup",
        "auto.offset.reset": "earliest",
        "metadata.fetch.timeout.ms": "30000"
    }
    
    kafka_consumer = FlinkKafkaConsumer(
        topics=['dbserver.bank.users', 'dbserver.bank.accounts', 'dbserver.bank.infouser'],
        deserialization_schema=SimpleStringSchema(),
        properties = properties
    )
    # kafka_consumer.set_start_from_earliest()
    json_stream = env.add_source(kafka_consumer).map(EventToJson())
    
    json_stream.key_by(lambda event: event['payload']['after']['id']).process(UserStateProcessFunction())
    env.execute()

if __name__ == '__main__':
    
    #conf = Configuration()
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    # URL al archivo JAR del conector Kafka
    jar_url = "file:///opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar"

    # Verificar si el archivo JAR existe
    if not os.path.isfile(jar_url[7:]):  # Elimina el prefijo "file://" para verificar la existencia del archivo
        print(f"Error: El archivo JAR del conector Kafka no se encuentra en {jar_url}. Por favor, verifica la ruta y vuelve a intentarlo.")
        sys.exit(1)  # Terminar el script con un código de error

    # Estrategia restart job
    # conf.set_string("restart-strategy", "fixed-delay")
    # conf.set_string("restart-strategy.fixed-delay.attempts", "3")
    # conf.set_string("restart-strategy.fixed-delay.delay", "10000")

    # Crear el entorno de ejecución con la configuración
    env = StreamExecutionEnvironment.get_execution_environment()

    env.add_jars(jar_url)

    print("start reading data from kafka")
    read_from_kafka(env)
