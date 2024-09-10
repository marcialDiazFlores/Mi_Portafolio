# from JUtil import *
from Mapa import *
import random
# Clase Base
# Campos:
# ju (instance)
# nom (str)
# hp (int)
# posicion (list)
class Base:
	# Constructor:
	def __init__(self, ju, nom):
		assert type(nom) == str
		# Inicializacion de campos:
		self._hp = 100 # Puntos de vida iniciales de la base
		self._ju = ju # Instancia de la clase JUtil
		self._nombre = nom # Nombre
		if self._nombre == "perro":
			self._posicion = [21, 12] # Posicion de la base
			self._ju.dibujarEstatico("base", 21*32-1, 12*32-1) # Base dibujada en la ventana
		elif self._nombre == "gato":
			self._posicion = [3, 4] # Posicion de la base
			self._ju.dibujarEstatico("base", 3*32-1, 4*32-1) # Base dibujada en la ventana

	# getPosicion: None -> list
	# Retorna una lista de python con dos parametros (x, y) que
	# indican la posicion de la base.
	# Ejemplo: Para ju = JUtil(), mapa = Mapa(ju, "simple") y base1 = Base(ju, "gato")
	# (con ventana y mapa dibujado), base1.getPosicion() retorna [3, 4]
	# Cuerpo del metodo:
	def getPosicion(self):
		return self._posicion

	# recibeAtaque: int -> bool
	# Recibe como parametro un entero, y modifica el campo hp
	# de la base restandole dicho entero.
	# Ejemplo: Para ju = JUtil(), mapa = Mapa(ju, "simple") y base1 = Base(ju, "gato")
	# (con ventana y mapa dibujado), base1.recibeAtaque(40) retorna True, y despues de esto
	# base1.recibeAtaque(60) retorna False.
	# Cuerpo del metodo:
	def recibeAtaque(self, dam):
		assert type(dam) == int
		assert dam >= 0
		self._hp -= dam
		if self._hp > 0:
			print ("Puntos de vida restantes: " + str(self._hp))
			return True
		else:
			self._hp = 0
			print ("Puntos de vida restantes: " + str(self._hp))
			return False

	# salida: None -> list
	# Retorna una salida aleatoria de la base (arriba, abajo, a la izquerda o
	# a la derecha de esta).
	# Ejemplo: Para ju = JUtil(), mapa = Mapa(ju, "simple") y base2 = Base(ju, "perro")
	# (con ventana y mapa dibujado), base2.salida() puede retornar [22, 12], [20, 12],
	# [21, 11] o [21, 13].
	# Cuerpo del metodo:
	def salida(self):
		n = random.randint(1, 4)
		if self._nombre == "perro":
			if n == 1:
				return [22, 12]
			elif n == 2:
				return [20, 12]
			elif n == 3:
				return [21, 11]
			elif n == 4:
				return [21, 13]
			else:
				return self.salida()
		elif self._nombre == "gato":
			if n == 1:
				return [4, 4]
			elif n == 2:
				return [2, 4]
			elif n == 3:
				return [3, 3]
			elif n == 4:
				return [3, 5]
			else:
				return self.salida()

# *Los tests de los metodos de esta clase se encuentran en un modulo aparte.