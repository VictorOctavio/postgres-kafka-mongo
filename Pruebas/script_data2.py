import psycopg2
from faker import Faker
import random
import time

# Configura tus propios parámetros de conexión a la base de datos
conn_params = "dbname='postgres' user='postgres' host='localhost' password='postgres'"

# Inicializa Faker
fake = Faker()
# Generar datos de Persona
def generar_persona():
    dni = ''.join([str(random.randint(0, 9)) for _ in range(8)])
    nombre = fake.first_name()
    apellido = fake.last_name()
    email = fake.email()
    return dni, nombre, apellido, email

# Generar datos de Auto
def generar_auto(id_persona):
    marcas = ['Audi', 'Ford', 'Toyota', 'BMW', 'Chevrolet', 'Honda', 'Hyundai', 'Volkswagen']
    marca = random.choice(marcas)
    modelo = fake.word().capitalize()
    patente = ''.join([random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789') for _ in range(5)])
    return marca, modelo, patente, id_persona


# Función para obtener el máximo ID de Persona
def obtener_max_id_persona(cur):
    cur.execute("SELECT MAX(id) FROM prueba.personas")
    max_id = cur.fetchone()[0]
    return max_id if max_id is not None else 0


# Función para insertar datos en la BD
def insertar_datos(cantidad=None):
    conn = psycopg2.connect(conn_params)
    cur = conn.cursor()
    try:
        max_id_persona = obtener_max_id_persona(cur)

        if cantidad is not None:  # Opción 1: Cantidad máxima de datos a generar
            for _ in range(cantidad):
                dni, nombre, apellido, email = generar_persona()
                cur.execute("INSERT INTO prueba.personas (DNI, Nombre, Apellido, Email) VALUES (%s, %s, %s, %s) RETURNING id", (dni, nombre, apellido, email))
                id_persona = cur.fetchone()[0]
                marca, modelo, patente, _ = generar_auto(id_persona)
                cur.execute("INSERT INTO prueba.autos (Marca, Modelo, Patente, id_Persona) VALUES (%s, %s, %s, %s)", (marca, modelo, patente, id_persona))
                conn.commit()
        else:  # Opción 2: Generar datos de forma constante
            while True:
                dni, nombre, apellido, email = generar_persona()
                #cur.execute("INSERT INTO prueba.personas (DNI, Nombre, Apellido, Email) VALUES (%s, %s, %s, %s) RETURNING id", (dni, nombre, apellido, email))
                #id_persona = cur.fetchone()[0]
                # Insertar una nueva persona solo si es necesario
                if max_id_persona == 0 or random.choice([True, False]):
                    cur.execute("INSERT INTO prueba.personas (DNI, Nombre, Apellido, Email) VALUES (%s, %s, %s, %s) RETURNING id", (dni, nombre, apellido, email))
                    max_id_persona = cur.fetchone()[0]
                id_persona = random.randint(1, max_id_persona)
                marca, modelo, patente, _ = generar_auto(id_persona)
                cur.execute("INSERT INTO prueba.autos (Marca, Modelo, Patente, id_Persona) VALUES (%s, %s, %s, %s)", (marca, modelo, patente, id_persona))
                conn.commit()
                #time.sleep(1)  # Pausa de 1 segundo entre inserciones para simular flujo constante de datos
                print("DATO INSERTADO")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        cur.close()
        conn.close()

insertar_datos()
# Ejemplo de uso
# insertar_datos(10)  # Inserta 10 personas y 10 autos
# insertar_datos()  # Inserta datos de forma constante hasta que se detenga el script
