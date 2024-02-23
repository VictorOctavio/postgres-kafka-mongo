import os
import logging
import json

from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import DataStream, StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
    KafkaSink,
    KafkaRecordSerializationSchema,
    DeliveryGuarantee,
)
from pyflink.datastream.formats.json import JsonRowSerializationSchema, JsonRowDeserializationSchema
from pymongo import MongoClient

# Variables de Entorno
RUNTIME_ENV = os.getenv("RUNTIME_ENV", "local")
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")


def define_workflow(kafka_stream: DataStream):
    # Aquí puedes definir cualquier transformación adicional que necesites antes de enviar los datos a MongoDB.
    # Por ejemplo, si necesitas transformar los datos a BSON o a otro formato, puedes hacerlo aquí.
    # Por ahora, simplemente pasaremos los datos tal como vienen.
    return kafka_stream


def insert_to_mongo(data):
    # Convertir la cadena de texto a un diccionario de Python
    data_dict = json.loads(data)
    
    # Extraer y renombrar los campos deseados
    transformed_data = {
        "Nombre": data_dict["payload"]["after"]["nombre"],
        "email": data_dict["payload"]["after"]["email"],
        "dni": data_dict["payload"]["after"]["dni"]
    }
    
    # Función para insertar datos en MongoDB
    client = MongoClient("mongodb+srv://root:root@cluster.esryp20.mongodb.net/?retryWrites=true&w=majority")
    db = client["mongodb"]
    collection = db["users"]
    collection.insert_one(transformed_data)
    
    
if __name__ == "__main__":
    """
    ## cluster execution
    docker exec jobmanager /opt/flink/bin/flink run \
        --python /tmp/src/prueba.py \
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
    
    # ---------- KAFKA ----------

    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_topics("dbserver1.public.users")
        .set_group_id("thisgroup")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # Crear los flujos de datos a partir de las fuentes Kafka configuradas
    kafka_stream = env.from_source(
        kafka_source, WatermarkStrategy.no_watermarks(), "kafka_source"
    )


    # Aquí definimos el flujo de trabajo para consumir de Kafka y pasar los datos a MongoDB
    define_workflow(kafka_stream).map(
        lambda d: insert_to_mongo(d), output_type=None
    )

    env.execute("kafka_to_mongo")
