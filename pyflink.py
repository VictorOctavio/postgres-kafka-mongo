from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.descriptors import Schema, Json, Kafka, FileSystem
from pyflink.table.udf import udf

from pymongo import MongoClient

# Configuración de Kafka
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "topic_name"

# Configuración de MongoDB
mongo_host = "localhost"
mongo_port = 27017
mongo_db = "database_name"
mongo_collection = "collection_name"

# Configuración de Flink
env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(env)

# Configuración del conector de Kafka
t_env.connect(
    Kafka()
    .version("universal")
    .topic(kafka_topic)
    .start_from_earliest()
    .property("bootstrap.servers", kafka_bootstrap_servers)
    .property("group.id", "flink_consumer_group")
).with_format(
    Json()
    .schema(
        Schema()
        .field("id", DataTypes.INT())
        .field("name", DataTypes.STRING())
        .field("lastname", DataTypes.STRING())
        .field("createdAt", DataTypes.TIMESTAMP())
        .field("accountActive", DataTypes.BOOLEAN())
    )
).with_schema(
    Schema()
    .field("id", DataTypes.INT())
    .field("name", DataTypes.STRING())
    .field("lastname", DataTypes.STRING())
    .field("createdAt", DataTypes.TIMESTAMP())
    .field("accountActive", DataTypes.BOOLEAN())
).create_temporary_table("kafka_table")

# Configuración del conector de MongoDB
t_env.connect(
    FileSystem().path("file:///tmp/mongo_sink")
).with_format(
    Json()
    .derive_schema()
).with_schema(
    Schema()
    .field("name", DataTypes.STRING())
    .field("lastname", DataTypes.STRING())
    .field("amount", DataTypes.DECIMAL(10, 2))
).create_temporary_table("mongo_table")

# Definición de la función para crear el modelo
@udf(input_types=[DataTypes.STRING(), DataTypes.STRING(), DataTypes.DECIMAL(10, 2)])
def create_model(name, lastname, amount):
    # Aquí puedes escribir la lógica para crear el modelo
    model = {
        "name": name,
        "lastname": lastname,
        "amount": amount
    }
    return model

# Definición de la tabla de salida
t_env.from_path("kafka_table") \
    .group_by("name, lastname") \
    .select("name, lastname, sum(amount) as total_amount") \
    .insert_into("mongo_table")

# Ejecución del programa
t_env.execute("Flink Python Test")