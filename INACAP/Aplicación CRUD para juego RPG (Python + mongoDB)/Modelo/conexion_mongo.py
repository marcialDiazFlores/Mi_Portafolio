from pymongo import MongoClient
from pymongo import DeleteOne

# Establecer la conexión con MongoDB
client = MongoClient("mongodb://localhost:27017")

# Obtener una referencia a la base de datos
db = client["RPG"]

# Obtener una referencia a la colección
collection = db["Personaje"]

# Realizar operaciones en la colección
filtro = {"nombre": "Ejemplo"}
result = collection.delete_one(filtro)


# Cerrar la conexión
client.close()
