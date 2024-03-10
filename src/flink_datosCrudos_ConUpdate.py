import os
import logging
import json
from datetime import datetime

from pyflink.common import WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import (
    DataStream, 
    StreamExecutionEnvironment, 
    RuntimeExecutionMode, 
    functions, 
    ExternalizedCheckpointCleanup,
    EmbeddedRocksDBStateBackend
)
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
    KafkaSink,
    KafkaRecordSerializationSchema,
    DeliveryGuarantee,
)
from pyflink.datastream.formats.json import JsonRowSerializationSchema, JsonRowDeserializationSchema
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.functions import RuntimeContext
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


def update_mongo_and_state(data, tabla, state):
    # Convertir la cadena de texto a un diccionario de Python
    data_dict = convert_do_dict(data)

    # Obtener el ID del dato
    data_id = data_dict["payload"]["after"]["id"]

    # Obtener el estado actual para el ID
    current_state = state.value()
    
    if current_state is not None and data_id in current_state:
        # Si el ID ya existe en el estado, actualizar el estado y MongoDB
        current_state[data_id].update(data_dict["payload"]["after"])
        state.update(current_state)
        update_mongo_document(tabla, data_id, current_state[data_id])
    else:
        # Si el ID no existe en el estado, guardar el estado y insertar en MongoDB
        current_state[data_id] = data_dict["payload"]["after"]
        state.update(current_state)
        insert_to_mongo(data, tabla)


def update_mongo_document(tabla, data_id, updated_data):
    # Convertir la cadena de texto a un diccionario de Python
    updated_data_dict = convert_do_dict(updated_data)

    # Crear un filtro para encontrar el documento correspondiente en MongoDB
    filter_condition = {"Data.ID": data_id}

    # Crear una actualización con los nuevos datos
    update_data = {"$set": updated_data_dict}

    # Conectar a MongoDB y actualizar el documento
    client = MongoClient("mongodb://192.168.200.8:27017/")
    db = client["interbase_mngdb"]
    collection = db["test_flink_total"]

    # Actualizar el documento en la colección correspondiente
    collection.update_one(filter_condition, update_data)


if __name__ == "__main__":
    """
    ## cluster execution
    docker exec jobmanager /opt/flink/bin/flink run \
        --python /tmp/src/flink_datosCrudos_ConUpdate.py \
        -d
        
    ## Deter y guardar SavePoint
    docker exec jobmanager /opt/flink/bin/flink stop \
        --type canonical [IdJOB] \
        -d
        
    ## Iniciar Job desde SavePoint
    docker exec jobmanager /opt/flink/bin/flink run \
        -s file:///tmp/src/savepoints/[FolderSavePoint] \
        --python /tmp/src/flink_datosCrudos_ConUpdate.py \
        -d
        
    ## Iniciar Job desde CheckPoint
    docker exec jobmanager /opt/flink/bin/flink run \
        -s file:///tmp/src/checkpoint/[FolderChakePoint]/[chk-id] \
        --python /tmp/src/flink_datosCrudos_ConUpdate.py \
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

    # Configuracion StateBackend
    env.set_state_backend(EmbeddedRocksDBStateBackend())

    # Configurar manejo de CheckPoints
    #env.get_checkpoint_config().enable_externalized_checkpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    #env.get_checkpoint_config().set_checkpoint_interval(60000)
    
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

    # ---------- FLUJO DE TRABAJO PRINCIPAL ----------


    # Definir los descriptores de estado para Persona y Autos
    persona_state_descriptor = ValueStateDescriptor(
        "persona_state", Types.STRING()
    )
    autos_state_descriptor = ValueStateDescriptor(
        "autos_state", Types.MAP(Types.STRING(), Types.MAP(Types.STRING(), Types.STRING()))
    )

    # Obtener el estado gestionado por Flink
    persona_state = RuntimeContext.get_state(persona_state_descriptor)
    autos_state = RuntimeContext.get_state(self="autos_state", state_descriptor=autos_state_descriptor)

    persona_state.value()
    autos_state.value()

    # Definir el flujo de trabajo para consumir de Kafka y pasar los datos de persona a MongoDB
    kafka_stream_persona.map(
        lambda d: update_mongo_and_state(d, "Persona", persona_state_descriptor),
        output_type=None
    )

    # Definir el flujo de trabajo para consumir de Kafka y pasar los datos de autos a MongoDB
    kafka_stream_autos.map(
        lambda d: update_mongo_and_state(d, "Autos", autos_state_descriptor),
        output_type=None
    )
    

    # ---------- EJECUCION DE LA TAREA ----------

    env.execute("kafka_to_mongo")