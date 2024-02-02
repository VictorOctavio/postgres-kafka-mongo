from pymongo import MongoClient

# Configuración de conexión a mongodb+srv://root:<password>@cluster.esryp20.mongodb.net/
mongo_client = MongoClient('mongodb+srv://root:root@cluster.esryp20.mongodb.net/?retryWrites=true&w=majority')
mongo_db = mongo_client['mongodb']
mongo_collection = mongo_db['accounts']

# Datos del usuario a insertar
user_data = {
    'name': 'Ejemplo',
    'age': 25,
    'email': 'ejemplo@example.com'
}

# Insertar el documento en la colección
result = mongo_collection.insert_one(user_data)

# Verificar el resultado
if result.inserted_id:
    print(f"Usuario insertado con ID: {result.inserted_id}")
else:
    print("Error al insertar el usuario.")
    
