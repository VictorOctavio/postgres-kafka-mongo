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


def define_workflow(kafka_stream_persona, kafka_stream_autos):
    # Fusionar los flujos de Kafka para trabajar con ambos
    merged_stream = kafka_stream_persona.union(kafka_stream_autos)

    # Realizar cualquier transformación adicional que necesites antes de enviar los datos a MongoDB.
    # Por ahora, simplemente pasaremos los datos tal como vienen.
    return merged_stream


def insert_to_mongo(data, tabla):
    # Convertir la cadena de texto a un diccionario de Python
    data_dict = convert_do_dict(data)

    # Campos comunes para ambas fuentes
    common_fields = {
        "Data": data,
        "Fecha_Alta": datetime.utcnow(),
        "Tabla": tabla
    }

    # Extraer y renombrar los campos deseados según la fuente (Persona o Autos)
    if tabla == "Persona":
        transformed_data = {
            "DNI": data_dict["payload"]["after"]["dni"],
            "Nombre": data_dict["payload"]["after"]["nombre"],
            "Apellido": data_dict["payload"]["after"]["apellido"],
            "Email": data_dict["payload"]["after"]["email"]
        }
    elif tabla == "Autos":
        transformed_data = {
            "Marca": data_dict["payload"]["after"]["marca"],
            "Modelo": data_dict["payload"]["after"]["modelo"],
            "Patente": data_dict["payload"]["after"]["patente"],
            "ID_Persona": data_dict["payload"]["after"]["id_persona"]
        }
    else:
        # Si la tabla no es reconocida, no se realiza ninguna transformación
        return

    # Combinar los campos comunes y los específicos de cada fuente
    transformed_data.update(common_fields)

    # Función para insertar datos en MongoDB
    #client = MongoClient("mongodb+srv://root:root@cluster.esryp20.mongodb.net/?retryWrites=true&w=majority")
    client = MongoClient("mongodb://localhost:27017/")
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
    
    
    # ---------- PROCESOS DE TRANSFORMACION Y UNION DE DATOS ----------

    # Aquí definimos el flujo de trabajo para consumir de Kafka y pasar los datos a MongoDB
    define_workflow(kafka_stream_persona, kafka_stream_autos).map(
        lambda d, tabla: insert_to_mongo(d, tabla), output_type=None
    )
    


    # ---------- EJECUCION DE LA TAREA ----------

    env.execute("kafka_to_mongo")