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


# Función para trabajar con el estado en PyFlink
class StatefulFunction(functions.MapFunction):
    def open(self, runtime_context: functions.RuntimeContext):
        # ---------- DEFINIR CONEXION A MONGO ----------
        self.client = MongoClient("mongodb+srv://root:root@cluster.esryp20.mongodb.net/?retryWrites=true&w=majority")
        self.db = self.client["mongodb"]
        self.collection = self.db["usuarios_3"]

    def close(self):
        # Cerrar la conexión a MongoDB
        if self.client:
            self.client.close()

    def map(self, value):
       

        # Si el usuario no existe, insertar un nuevo documento en MongoDB
        transformed_data = {
            "ID": value.get("payload", {}).get("after", {}).get("id", None),
            "Nombre": value.get("payload", {}).get("after", {}).get("nombre", None),
            "email": value.get("payload", {}).get("after", {}).get("email", None),
            "dni": value.get("payload", {}).get("after", {}).get("dni", None)
        }
   
        #if "payload" in transformed_data:
        #    del transformed_data["payload"]
        #if "schema" in transformed_data:
        #    del transformed_data["schema"]
   
        # ---------- INSERTAR DATOS EN MONGO ----------
        self.collection.insert_one(transformed_data)



if __name__ == "__main__":
    """
    ## cluster execution
    docker exec jobmanager /opt/flink/bin/flink run \
        --python /tmp/src/user.py \
        -d
        
    ## Deter y guardar SavePoint
    docker exec jobmanager /opt/flink/bin/flink stop \
        --type canonical [IdJOB] \
        -d
        
    ## Iniciar Job desde SavePoint
    docker exec jobmanager /opt/flink/bin/flink run \
        -s file:///tmp/src/savepoints/[FolderSavePoint] \
        --python /tmp/src/user.py \
        -d
        
    ## Iniciar Job desde CheckPoint
    docker exec jobmanager /opt/flink/bin/flink run \
        -s file:///tmp/src/checkpoint/[FolderChakePoint] \
        --python /tmp/src/user.py \
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

    # Configuración de CheckPoints
    env.enable_checkpointing(60000)
   
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
   
   

    # ---------- DEFINICION DE LOS FLUJOS DE DATOS ----------
   
    # Crear los flujos de datos a partir de las fuentes Kafka configuradas
    kafka_stream_users = env.from_source(
        kafka_source_users, WatermarkStrategy.no_watermarks(), "kafka_source_users"
    )
   
   
    # ---------- PROCESOS DE TRANSFORMACION A DICCIONARIO PYTHON ----------

    # Convertir los datos de Kafka a diccionarios de Python
    ks_users_dict = kafka_stream_users.map(convert_do_dict)
   

    # ---------- ESPECIFICAR COMO AGRUPAR LOS DATOS ----------

    users_with_key = ks_users_dict.key_by(lambda x: x["payload"]["after"]["id"])


    # ---------- TRANSFORMAR Y ENVIAR DATOS A MONGO ----------


    # Aplicar la función de estado a los datos combinados
    users_with_key.map(StatefulFunction(), output_type=None)
   


    # ---------- EJECUCION DE LA TAREA ----------

    env.execute("kafka_to_mongo")
