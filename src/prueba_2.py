import os
import logging
import json

from pyflink.common import WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import DataStream, StreamExecutionEnvironment, RuntimeExecutionMode, functions
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
    KafkaSink,
    KafkaRecordSerializationSchema,
    DeliveryGuarantee,
)
from pyflink.datastream.formats.json import JsonRowSerializationSchema, JsonRowDeserializationSchema
from pyflink.datastream.state import ValueStateDescriptor
from pymongo import MongoClient



# Variables de Entorno
RUNTIME_ENV = os.getenv("RUNTIME_ENV", "local")
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")


# Convertir los datos de Kafka a diccionarios de Python
def convert_do_dict(data):
    return json.loads(data)



# Función para combinar datos de usuarios y autos
def combine_user_auto_data(user_data, auto_data):
    #user_id = user_data["payload"]["after"]["id"]
    user_id = user_data.get("payload", {}).get("after", {}).get("id", None)
    #auto_id = auto_data["payload"]["after"]["usuario_id"]
    auto_id = auto_data.get("payload", {}).get("after", {}).get("usuario_id", None)
    if user_id == auto_id:
        user_data["autos"] = user_data.get("autos", []) + [auto_data["payload"]["after"]]        
    return user_data



# Función para trabajar con el estado en PyFlink
class StatefulFunction(functions.MapFunction):
    def open(self, runtime_context: functions.RuntimeContext):
        # Definir el descriptor del estado
        state_descriptor = ValueStateDescriptor("user_state", Types.PICKLED_BYTE_ARRAY())
        # Acceder al estado aquí
        self.state = runtime_context.get_state(state_descriptor)

    def map(self, value):
        # ---------- DEFINIR CONEXION A MONGO ----------
        client = MongoClient("mongodb+srv://root:root@cluster.esryp20.mongodb.net/?retryWrites=true&w=majority")
        db = client["mongodb"]
        collection = db["usuarios"]

        # Lógica para trabajar con el estado
        # Por ejemplo, guardar o actualizar el estado
        user_id = value.get("payload", {}).get("after", {}).get("id", None)
        if user_id is not None:
            if self.state.value() is not None:
                # Si el usuario existe en el estado, actualizar el estado
                existing_state = self.state.value()
                existing_state.update(value)
                self.state.update(existing_state)
            
                # Actualizar el documento correspondiente en MongoDB
                transformed_data = self.state.value()
                transformed_data.update(value)
                logging.info(transformed_data)
                # Asegúrate de que el campo '_id' sea único y no se modifique
                if "ID" in transformed_data:
                    del transformed_data["ID"]
                if "payload" in transformed_data:
                    del transformed_data["payload"]
                if "schema" in transformed_data:
                    del transformed_data["schema"]
                # ---------- INSERTAR DATOS ACTUALIZADOS EN MONGO ----------
                collection.update_one({"ID": user_id}, {"$set": transformed_data}, upsert=True)
            else:
                # Si el usuario no existe, insertar un nuevo documento en MongoDB
                transformed_data = {
                    "ID": value.get("payload", {}).get("after", {}).get("id", None),
                    "Nombre": value.get("payload", {}).get("after", {}).get("nombre", None),
                    "email": value.get("payload", {}).get("after", {}).get("email", None),
                    "dni": value.get("payload", {}).get("after", {}).get("dni", None),
                    "autos": value.get("autos", [])
                }
    
                if "payload" in transformed_data:
                    del transformed_data["payload"]
                if "schema" in transformed_data:
                    del transformed_data["schema"]
    
                # ---------- INSERTAR DATOS EN MONGO ----------
                collection.insert_one(transformed_data)
                # Guardar el estado para el nuevo usuario
                self.state.update(transformed_data)


    
if __name__ == "__main__":
    """
    ## cluster execution
    docker exec jobmanager /opt/flink/bin/flink run \
        --python /tmp/src/prueba_2.py \
        -d
    """

    # ----------CONFIGURACIONES FLINK----------

    # Configuración del registro de eventos
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d:%(levelname)s:%(name)s:%(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logging.info(f"RUNTIME_ENV - {RUNTIME_ENV}, BOOTSTRAP_SERVERS - {BOOTSTRAP_SERVERS}")

    # Obtener el entorno de ejecución de PyFlink
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Configurar el modo de ejecución a 'STREAMING'
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    
    # Configurar el paralelismo del entorno (opcional)
    # env.set_parallelism(5)


    
    # ---------- DEFINIR FUENTES DE KAFKA ----------

    # Fuentes de Kafka
    kafka_source_users = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_topics("dbserver1.public.users")
        .set_group_id("thisgroup")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    
    kafka_source_autos = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_topics("dbserver1.public.autos")
        .set_group_id("thisgroup")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    
    
    

    # ---------- DEFINICION DE LOS FLUJOS DE DATOS ----------
    
    # Crear los flujos de datos a partir de las fuentes Kafka configuradas
    kafka_stream_users = env.from_source(
        kafka_source_users, WatermarkStrategy.no_watermarks(), "kafka_source_users"
    )
    
    kafka_stream_autos = env.from_source(
        kafka_source_autos, WatermarkStrategy.no_watermarks(), "kafka_source_autos"
    )
    
    
    
    # ---------- PROCESOS DE TRANSFORMACION Y UNION DE DATOS ----------

    # Convertir los datos de Kafka a diccionarios de Python
    ks_users_dict = kafka_stream_users.map(convert_do_dict)
    ks_autos_dict = kafka_stream_autos.map(convert_do_dict)
    
    # Combinar los flujos de datos de usuarios y autos
    combined_stream = ks_users_dict.union(ks_autos_dict)
    
    

    # ---------- PROCESOS DE COMBINACION DE LOS DATOS COMBINADOS ----------

    # Aplicar la función de combinación a los datos combinados
    combined_data_stream = combined_stream.key_by(lambda x: x["payload"]["after"]["id"]).reduce(combine_user_auto_data)
    
    

    # ---------- PROCESOS DE MANEJO DE ESTADOS DE LOS DATOS E INSERCION/ACTUALIZACION DE LOS DATOS EN MONGO ----------


    combined_data_stream = combined_data_stream.key_by(lambda x: x["payload"]["after"]["id"])


    # Aplicar la función de estado a los datos combinados
    combined_data_stream.map(StatefulFunction(), output_type=None)
    


    # ---------- EJECUCION DE LA TAREA ----------

    env.execute("kafka_to_mongo")
