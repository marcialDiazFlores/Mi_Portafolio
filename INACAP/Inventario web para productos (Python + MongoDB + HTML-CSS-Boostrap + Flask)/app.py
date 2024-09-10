import subprocess

# Instalación de flask y pymongo utilizando pip

modules_to_install = ['flask', 'pymongo']
for module in modules_to_install:
    subprocess.check_call(['pip', 'install', module])

from flask import Flask, render_template, request, Response, jsonify, redirect, url_for
import database as dbase
from product import Product
import json

# Instancia de BdD
db = dbase.dbConnection()

# Inicializa flask y crea una instancia de una aplicación web
app = Flask(__name__)

# Rutas de la aplicación:

# Ruta de la vista principal de la app
@app.route('/')

# Función asociada a esta ruta
def home():
    products = db['products']
    productsReceived = products.find()
    return render_template('index.html', products = productsReceived) # envía los datos de la BdD hacia el front end

# Ruta para guardar datos en la BdD y método de recolección
@app.route('/products', methods=['POST'])

# Función para añadir producto a la BdD
def addProduct():
    products = db['products']
    name = request.form['name']
    price = request.form['price']
    quantity = request.form['quantity']
    description = request.form['description']

    if name and price and quantity and description:
        product = Product(name, price, quantity, description)
        products.insert_one(product.toDBCollection())
        response = jsonify({
            'name' : name,
            'price' : price,
            'quantity' : quantity,
            'description' : description
        })
        return redirect(url_for('home'))
    else:
        return notFound()


# Ruta de borrado
@app.route('/delete/<string:product_name>')

# Función de borrado
def delete(product_name):
    products = db['products']
    products.delete_one({'name': product_name}) # búsqueda por nombre según parámetro obtenido desde la URL
    return redirect(url_for('home'))

# Ruta de modificación de registros
@app.route('/edit/<string:product_name>', methods = ['POST'])

# Función de modificación de registros
def edit(product_name):
    products = db['products']

    # obtención de datos desde formulario:

    name = request.form['name']
    price = request.form['price']
    quantity = request.form['quantity']
    description = request.form['description']

    # creación del objeto Product
    if name and price and quantity and description:
        products.update_one({'name': product_name}, {'$set': {'name': name, 'price': price, 'quantity': quantity, 'description': description}}) # actualización en mongodb
        response = jsonify({'message': 'Producto ' + product_name + ' actualizado correctamente'})
        return redirect(url_for('home'))
    
# Ruta de manejo de error
@app.errorhandler(404)

# Función de mensaje de error
def notFound(error=None):
    message = {
        'error': 'Ruta no encontrada',
        'status': 404
    }
    response = jsonify(message)
    response.status_code = 404
    return response

# Si se ejecuta el archivo como un módulo principal, se ejecutará la aplicación
if (__name__ == "__main__"):
    app.run(debug=True, port=4000) # Ejecuta la app y cada vez que se haga un cambio en el código se reinicia el servidor, puerto: 4000
