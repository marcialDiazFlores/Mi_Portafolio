# from JUtil import *
from Mapa import *
# Clase Item
# Campos:
# ju (instance)
# nom (str)
# posicion (list)
# sp (referencia del dibujo)
class Item:
	# Constructor:
	def __init__(self, ju, nom, x, y):
		assert type(nom) == str
		assert type(x) == int and type(y) == int
		assert x >= 0 and x <= 24
		assert y >= 0 and y <= 16
		# Inicializacion de campos:
		self._ju = ju # Instancia de la clase JUtil
		self._nom = nom # Nombre
		self._posicion = [x, y] # Posicion inicial del item
		if self._nom == "Espada":
			self._sp = ju.dibujarDinamico("sword", x*32-1, y*32-1) # Referencia del item
		elif self._nom == "Escudo":
			self._sp = ju.dibujarDinamico("shield", x*32-1, y*32-1) # Referencia del item
		elif self._nom == "Pocion":
			self._sp = ju.dibujarDinamico("potion", x*32-1, y*32-1) # Referencia del item
		elif self._nom == "Botas":
			self._sp = ju.dibujarDinamico("boots", x*32-1, y*32-1) # Referencia del item

	# getPosicion: None -> list
	# Retorna la posicion del item en una lista de python.
	# Ejemplo:
	# Cuerpo del metodo:
	def getPosicion(self):
		return self._posicion

	# getNombre: None -> str
	# Retorna un string con el nombre del objeto.
	# Ejemplo:
	# Cuerpo del metodo:
	def getNombre(self):
		return self._nom

	# destruir: None -> None
	# Cambia la posicion del objeto a una invisible en el mapa y lo mueve
	# a dicha posicion.
	# Ejemplo:
	# Cuerpo del metodo:
	def destruir(self):
		self._posicion = [100**100, 100**100]
		self._ju.mover(self._sp, (100**100)*32-1, (100**100)*32-1)

# *Los tests de los metodos de esta clase se encuentran en un modulo aparte.