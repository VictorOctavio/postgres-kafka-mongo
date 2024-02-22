import os
import datetime
import logging

from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import DataStream
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream import TimeCharacteristic
from pyflink.datastream.functions import ProcessFunction
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
    KafkaSink,
    KafkaRecordSerializationSchema,
    DeliveryGuarantee,
)
from pyflink.datastream.formats.json import JsonRowSerializationSchema, JsonRowDeserializationSchema
from pyflink.table import DataTypes
from pyflink.table.udf import udf
from pyflink.java_gateway import (java_import, logger, JavaGateway, GatewayParameters, CallbackServerParameters, get_gateway, JavaObject)
from pyflink.table import StreamTableEnvironment

# Variables de Entorno
RUNTIME_ENV = os.getenv("RUNTIME_ENV", "local")
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")

# Configurar la fuente Kafka para los datos del Esquema Public - Deserializacion Simple
def configure_kafka_consumer():
    source_public = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_topics("dbserver1.public.users")
        .set_group_id("thisgroup")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    return source_public

def configure_transform_udf():
    # Definir una función UDF para transformar los datos si es necesario
    @udf(input_types=[DataTypes.STRING()], result_type=DataTypes.STRING())
    def transform_udf(value):
        # Realizar transformaciones según sea necesario
        return value.upper()

    return transform_udf


def configure_mongo_connector():
    # Obtener la puerta de enlace de Java para la ejecución de Flink
    gateway = get_gateway()

    # Obtener una referencia a la tabla de entorno de transmisión (DataStreamTableEnvironment)
    table_env = StreamTableEnvironment.create(env)
    table_env_java = gateway.jvm.org.apache.flink.table.api.bridge.java
    #table_env_java = gateway.jvm.org.apache.flink.table.api.bridge.py4j.PyFlinkStreamTableEnvironment(table_env._j_tenv)

    # Crear un tipo de dato de fila para definir el esquema de la tabla
    #row_type = table_env_java.fromDataTypeToTypeInfo(table_env.create_type_info(DataTypes.ROW([DataTypes.STRING(), DataTypes.STRING()])))
    
    #Alternativa
    """row_type = table_env_java.create_type_info(
        table_env_java.from_data_type(
            DataTypes.ROW([DataTypes.FIELD("field1", DataTypes.STRING()), DataTypes.FIELD("field2", DataTypes.STRING())])
        )
    )"""

    # Configurar la conexión a MongoDB - Obtener el conector MongoDB
    mongo_connector = gateway.jvm.org.apache.flink.connector.mongodb.sink.MongoSink.builder() \
        .setUri("mongodb+srv://root:root@cluster.esryp20.mongodb.net/?retryWrites=true&w=majority") \
        .setDatabase("mongodb") \
        .setCollection("users") \
        .setBatchSize(1000) \
        .setBatchIntervalMs(1000) \
        .setMaxRetries(3) \
        .setDeliveryGuarantee(gateway.jvm.org.apache.flink.connector.base.DeliveryGuarantee.AT_LEAST_ONCE) \
        .setSerializationSchema(lambda input, context: gateway.jvm.com.mongodb.client.model.InsertOneModel(gateway.jvm.org.bson.BsonDocument.parse(input))) \
        .build()
    
    """.setSerializationSchema(
            lambda input, context: gateway.jvm.com.mongodb.client.model.InsertOneModel(gateway.jvm.org.bson.BsonDocument.parse(input))
        ) """
    # Obtener una instancia de la tabla de Java correspondiente al esquema de la tabla
    table_java = table_env_java.fromDataStream(transformed_stream, ['field1', 'field2'])

    # Emitir los datos transformados a MongoDB
    table_java.executeInsert(mongo_connector)

    return mongo_connector

# MAIN
if __name__ == "__main__":
    """
    ## cluster execution
    docker exec jobmanager /opt/flink/bin/flink run \
        --python /tmp/src/Kafka_mongo.py \
        -d
    """
    # ----------CONFIGURACIONES FLINK----------

    # Configuración del registro de eventos
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d:%(levelname)s:%(name)s:%(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Mostrar la configuración de las variables de entorno
    logging.info(f"RUNTIME_ENV - {RUNTIME_ENV}, BOOTSTRAP_SERVERS - {BOOTSTRAP_SERVERS}")

    # Obtener el entorno de ejecución de PyFlink
    env = StreamExecutionEnvironment.get_execution_environment()

    # Configurar el modo de ejecución a 'STREAMING'
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)

    # Configurar el paralelismo del entorno (opcional)
    # env.set_parallelism(5)

    # ---------- KAFKA ----------

    # Configurar el consumidor de Kafka
    kafka_consumer = configure_kafka_consumer()
    # Crear los flujos de datos a partir de las fuentes Kafka configuradas
    public_stream = env.from_source(kafka_consumer, WatermarkStrategy.no_watermarks(), "kafka_consumer")

    # ----------TRANSFORMACION DE DATOS ----------
    # Configurar la función UDF de transformación
    transform_udf = configure_transform_udf()
    transformed_stream = public_stream.map(transform_udf)

    # ---------- MONGO ----------

    # Configurar el conector MongoDB
    mongo_connector = configure_mongo_connector()

    """######### EJECUCION DEL JOB #########"""

    # Ejecutar el programa de Flink
    env.execute("KafkaToMongoJob")
