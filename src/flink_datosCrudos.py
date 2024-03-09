import os
import logging
import json
from datetime import datetime

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


def insert_to_mongo(data, tabla):
    # Convertir la cadena de texto a un diccionario de Python
    data_dict = convert_do_dict(data)

    # Extraer y renombrar los campos deseados según la fuente (Persona o Autos)
    if tabla == "Persona":
        transformed_data = {
            "Data": {
                "DNI": data_dict["payload"]["after"]["dni"],
                "Nombre": data_dict["payload"]["after"]["nombre"],
                "Apellido": data_dict["payload"]["after"]["apellido"],
                "Email": data_dict["payload"]["after"]["email"]
            }
        }
    elif tabla == "Autos":
        transformed_data = {
            "Data": {
                "Marca": data_dict["payload"]["after"]["marca"],
                "Modelo": data_dict["payload"]["after"]["modelo"],
                "Patente": data_dict["payload"]["after"]["patente"],
                "ID_Persona": data_dict["payload"]["after"]["id_persona"]
            }
        }
    else:
        # Si la tabla no es reconocida, no se realiza ninguna transformación
        return

    # Agregar campos adicionales
    transformed_data.update({
        "Fecha_Alta": datetime.now(),
        "Tabla": tabla
    })

    # Función para insertar datos en MongoDB
    #client = MongoClient("mongodb+srv://root:root@cluster.esryp20.mongodb.net/?retryWrites=true&w=majority")
    client = MongoClient("mongodb://192.168.200.8:27017/")
    db = client["interbase_mngdb"]
    collection = db["test_flink_total"]
    collection.insert_one(transformed_data)


if __name__ == "__main__":
    """
    ## cluster execution
    docker exec jobmanager /opt/flink/bin/flink run \
        --python /tmp/src/flink_datosCrudos.py \
        -d
        
    ## Deter y guardar SavePoint
    docker exec jobmanager /opt/flink/bin/flink stop \
        --type canonical [IdJOB] \
        -d
        
    ## Iniciar Job desde SavePoint
    docker exec jobmanager /opt/flink/bin/flink run \
        -s file:///tmp/src/savepoints/[FolderSavePoint] \
        --python /tmp/src/flink_datosCrudos.py \
        -d
        
    ## Iniciar Job desde CheckPoint
    docker exec jobmanager /opt/flink/bin/flink run \
        -s file:///tmp/src/checkpoint/[FolderChakePoint] \
        --python /tmp/src/flink_datosCrudos.py \
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
    #env.enable_checkpointing(60000)

    
    # ---------- DEFINIR FUENTES DE KAFKA ----------

    # Fuentes de Kafka
    kafka_prueba_persona= (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_topics("dbserver1.prueba.personas")
        .set_group_id("thisgroup")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    
    kafka_prueba_autos = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_topics("dbserver1.prueba.autos")
        .set_group_id("thisgroup")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    
    # ---------- DEFINICION DE LOS FLUJOS DE DATOS ----------
    
    # Crear los flujos de datos a partir de las fuentes Kafka configuradas
    kafka_stream_persona = env.from_source(
        kafka_prueba_persona, WatermarkStrategy.no_watermarks(), "kafka_prueba_persona"
    )
    
    kafka_stream_autos = env.from_source(
        kafka_prueba_autos, WatermarkStrategy.no_watermarks(), "kafka_prueba_autos"
    )
    
    
    # ---------- PROCESO UNION DE FLUJO DE DATOS ----------


    combined_stream = kafka_stream_persona.union(kafka_stream_autos)


    # ---------- FLUJO DE TRABAJO PRINCIPAL ----------


    # Definir el flujo de trabajo para consumir de Kafka y pasar los datos de persona a MongoDB
    kafka_stream_persona.map(
        lambda d: insert_to_mongo(d, "Persona"), output_type=None
    )

    # Definir el flujo de trabajo para consumir de Kafka y pasar los datos de autos a MongoDB
    kafka_stream_autos.map(
        lambda d: insert_to_mongo(d, "Autos"), output_type=None
    )
    


    # ---------- EJECUCION DE LA TAREA ----------

    env.execute("kafka_to_mongo")