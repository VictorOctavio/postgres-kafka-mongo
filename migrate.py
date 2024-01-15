import psycopg2
from pymongo import MongoClient
from bson import ObjectId

# Configuración de PostgreSQL
pg_config = {
    'host': 'localhost',
    'port': 5433,
    'database': 'postgres',
    'user': 'postgres',
    'password': 'postgres'
}

# Configuración de MongoDB
mongo_config = {
    'host': 'cluster0-shard-00-00.9baupcr.mongodb.net,cluster0-shard-00-01.9baupcr.mongodb.net,cluster0-shard-00-02.9baupcr.mongodb.net',
    'port': 27017,
    'username': 'root',
    'password': 'root',
    'database': 'mongodb',
    'ssl': True,
}

def migrate_data(collection_name):
    # Conexión a PostgreSQL
    pg_conn = psycopg2.connect(**pg_config)
    pg_cursor = pg_conn.cursor()

    # Conexión a MongoDB
    mongo_client = MongoClient('mongodb+srv://root:root@cluster.esryp20.mongodb.net/')
    mongo_db = mongo_client[mongo_config['database']]

    try:
        # Consulta PostgreSQL para obtener datos
        pg_cursor.execute("SELECT * FROM users")
        rows = pg_cursor.fetchall()

        # Migración a MongoDB
        for row in rows:
            document = {
                '_id': ObjectId(),  # Genera un nuevo ObjectId para cada documento
                'id': row[0],
                'nombre': row[1],
                'email': row[2],
                'dni': row[3],
                # Agrega más campos según sea necesario
            }

            mongo_db[collection_name].insert_one(document)

        print("Migración completada con éxito.")

    except Exception as e:
        print(f"Error durante la migración: {e}")

    finally:
        # Cierra las conexiones
        pg_cursor.close()
        pg_conn.close()
        mongo_client.close()

if __name__ == "__main__":
    collection_name = "users"
    migrate_data(collection_name)
