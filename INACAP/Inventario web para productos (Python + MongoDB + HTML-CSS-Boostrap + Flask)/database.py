# Archivo de conexión con la base de datos

from pymongo import MongoClient

# MONGO_URI = 'mongodb://localhost:27017'
# ca = certifi.where()

# Función de conexión con la BdD
def dbConnection():
    try:
        client = MongoClient('mongodb://localhost:27017')
        db = client["bdd_productos"]
    except ConnectionError:
        print('Error de conexión con la bdd')
    return db
