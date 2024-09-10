# from JUtil import *
from Mapa import *
# Clase Jugador
# Campos:
# ju (instance)
# nom (str)
# posicion (list)
# hp (int)
# atk (int)
# def (int)
# vel (int)
# sp (referencia de dibujo)
class Jugador:
	# Constructor:
	def __init__(self, ju, nom):
		assert type(nom) == str
		# Inicializacion de campos:
		self._ju = ju # Instancia de la clase JUtil
		self._nom = nom # Nombre
		if self._nom == "gato":
			self._posicion = [4, 4] # Posicion inicial del jugador
			self._hp = 130 # Puntos de vida iniciales del jugador
			self._atk = 32 # Puntos de ataque iniciales del jugador
			self._def = 28 # Puntos de defensa iniciales del jugador
			self._vel = 8 # Puntos de velocidad iniciales del jugador
			self._sp = self._ju.dibujarDinamico("cat", 4*32-1, 4*32-1) # Referencia del dibujo
		elif self._nom == "perro":
			self._posicion = [20, 12] # Posicion inicial del jugador
			self._hp = 100 # Puntos de vida iniciales del jugador
			self._atk = 40 # Puntos de ataque iniciales del jugador
			self._def = 32 # Puntos de defensa iniciales del jugador
			self._vel = 6 # Puntos de velocidad iniciales del jugador
			self._sp = self._ju.dibujarDinamico("dog", 20*32-1, 12*32-1) # Referencia del dibujo

	# mover: int int -> None
	# Recibe como parametros dos enteros x e y que simbolizan una posicion en
	# el mapa, luego cambia el campo de posicion de la clase a dicha posicion y 
	# mueve al personaje a dicha posicion en el mapa.
	# Ejemplo: Para ju = JUtil(), mapa = Mapa(ju, "simple") y jugador1 = Jugador(ju, "gato")
	# (con ventana y mapa dibujado), jugador1.mover(4, 5) cambia jugador1._posicion a [4, 5].
	# Cuerpo del metodo:
	def mover(self, x, y):
		assert type(x) == int and type(y) == int
		assert x >= 0 and x <= 24
		assert y >= 0 and y <= 16
		self._posicion = [x, y]
		self._ju.mover(self._sp, x*32-1, y*32-1)
	
	# getPosicion: None -> list
	# Retorna la posicion del jugador en una lista de python.
	# Ejemplo: Para ju = JUtil(), mapa = Mapa(ju, "simple") y jugador1 = Jugador(ju, "gato")
	# (con ventana y mapa dibujado), jugador1.getPosicion() retorna [4, 4].
	# Cuerpo del metodo:
	def getPosicion(self):
		return self._posicion

	# getAtaque: None -> int
	# Retorna el valor de los puntos de ataque del jugador.
	# Ejemplo: Para ju = JUtil(), mapa = Mapa(ju, "simple") y jugador1 = Jugador(ju, "gato")
	# (con ventana y mapa dibujado), jugador1.getAtaque() retorna 32.
	# Cuerpo del metodo:
	def getAtaque(self):
		return self._atk

	# getDefensa: None -> int
	# Retorna el valor de los puntos de defensa del jugador.
	# Ejemplo: Para ju = JUtil(), mapa = Mapa(ju, "simple") y jugador1 = Jugador(ju, "gato")
	# (con ventana y mapa dibujado), jugador1.getDefensa() retorna 28.
	# Cuerpo del metodo:
	def getDefensa(self):
		return self._def

	# getVida: None -> int
	# Retorna el valor de los puntos de vida del jugador.
	# Ejemplo: Para ju = JUtil(), mapa = Mapa(ju, "simple") y jugador1 = Jugador(ju, "gato")
	# (con ventana y mapa dibujado), jugador1.getVida() retorna 130.
	# Cuerpo del metodo:
	def getVida(self):
		return self._hp

	# getVelocidad: None -> int
	# Retorna el valor de los puntos de velocidad del jugador.
	# Ejemplo: Para ju = JUtil(), mapa = Mapa(ju, "simple") y jugador1 = Jugador(ju, "gato")
	# (con ventana y mapa dibujado), jugador1.getVelocidad() retorna 8.
	# Cuerpo del metodo:
	def getVelocidad(self):
		return self._vel

	# recibeAtaque: int -> bool
	# Recibe como parametro un entero, y modifica el campo hp
	# del jugador restandole dicho entero.
	# Ejemplo: Para ju = JUtil(), mapa = Mapa(ju, "simple") y jugador1 = Jugador(ju, "gato")
	# (con ventana y mapa dibujado), jugador1.recibeAtaque(40) retorna True, y despues de esto
	# jugador1.recibeAtaque(70) retorna False.
	# Cuerpo del metodo:
	def recibeAtaque(self, dam):
		assert type(dam) == int
		assert dam >= 0
		self._hp -= dam
		if self._hp > 0:
			return True
		else:
			self._hp = 0
			return False

	# mejora: str int -> None
	# Recibe como parametros un string que simboliza un atributo del jugador a mejorar y un entero
	# que indica la cantidad en que aumentara dicho atributo.
	# Ejemplo: Para ju = JUtil(), mapa = Mapa(ju, "simple") y jugador1 = Jugador(ju, "gato")
	# (con ventana y mapa dibujado), jugador1.mejora("D", 20) aumenta jugador1._def en 20 puntos,
	# dejando def en 48, y despues de esto jugador1.mejora("D", 30) aumento jugador1._def en 30 puntos,
	# dejando def en 78.
	# Cuerpo del metodo:
	def mejora(self, tipo, cant):
		assert type(tipo) == str
		assert type(cant) == int 
		assert cant >= 0
		if tipo == "A":
			self._atk += cant
		elif tipo == "D":
			self._def += cant
		elif tipo == "H":
			self._hp += cant
		elif tipo == "V":
			self._vel += cant

# *Los tests de los metodos de esta clase se encuentran en un modulo aparte.