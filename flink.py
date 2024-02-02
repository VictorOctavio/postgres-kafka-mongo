from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.descriptors import Schema, OldCsv, FileSystem

# Configuración del entorno de ejecución y la tabla
env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
env = StreamExecutionEnvironment.get_execution_environment()
table_env = StreamTableEnvironment.create(env, environment_settings=env_settings)

# Definición de la tabla de entrada (vista de PostgreSQL)
table_env.execute_sql("""
CREATE TABLE vista_postgres (
    id_user INT,
    nombreUsuario STRING,
    id_auto INT,
    marcaVehiculo STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://localhost:5432/mydatabase',
    'table-name' = 'nombre_de_la_vista',
    'username' = 'myuser',
    'password' = 'mypassword',
    'driver' = 'org.postgresql.Driver'
)
""")

# Definición de la tabla de salida para MongoDB
table_env.execute_sql("""
CREATE TABLE mongo_sink (
    id_user INT,
    nombreUsuario STRING,
    id_auto INT,
    marcaVehiculo STRING
) WITH (
    'connector' = 'mongodb',
    'uri' = 'mongodb://localhost:27017',
    'database' = 'mydatabase',
    'collection' = 'nombre_coleccion_mongo'
)
""")

# Query para insertar datos en MongoDB
table_env.execute_sql("""
INSERT INTO mongo_sink
SELECT id_user, nombreUsuario, id_auto, marcaVehiculo
FROM vista_postgres
""")

# Ejecución del job de Flink
table_env.execute("My Flink Job")
