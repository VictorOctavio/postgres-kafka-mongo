import psycopg2
from faker import Faker
import random

# Configuración de Faker para generar datos en español
fake = Faker('es_ES')

# Datos de conexión a la base de datos
conn_params = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost'
}

# Función para generar un DNI falso
def generar_dni():
    return ''.join([str(random.randint(0, 9)) for _ in range(8)])

# Función para generar una patente falsa
def generar_patente():
    return ''.join([random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789') for _ in range(5)])

# Conectar a la base de datos
conn = psycopg2.connect(**conn_params)
cur = conn.cursor()

# Número de personas y autos a generar
num_personas = 100
num_autos = 200

# Generar y insertar datos de personas
for _ in range(num_personas):
    dni = generar_dni()
    nombre = fake.first_name()
    apellido = fake.last_name()
    email = fake.email()

    cur.execute("INSERT INTO prueba.personas (dni, nombre, apellido, email) VALUES (%s, %s, %s, %s)", (dni, nombre, apellido, email))

# Generar y insertar datos de autos
for _ in range(num_autos):
    marca = fake.company()
    modelo = fake.word()
    patente = generar_patente()
    id_persona = random.randint(1, num_personas)  # Asegurarse de que el id_persona no sea mayor al número de personas

    cur.execute("INSERT INTO prueba.autos (marca, modelo, patente, id_persona) VALUES (%s, %s, %s, %s)", (marca, modelo, patente, id_persona))

# Confirmar cambios y cerrar conexión
conn.commit()
cur.close()
conn.close()
