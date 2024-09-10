import pygame
from Mapa import *
from Base import *
from Jugador import *
from Item import *
import sys
import random
# Clase Mapa
# Campos:
# ju (instance)
# mapa (instance)
# basePerro (instance)
# baseGato (instance)
# perro (instance)
# gato (instance)
# items (list)
# turno (int)
# juego (bool) 
class Main:
	# Constructor:
	def __init__(self):
		# Inicializacion de campos:
		# Inicialización de Pygame
		pygame.init()
		# Creación de la ventana
		ventana = pygame.display.set_mode((24*32, 16*32))
		m = random.randint(1, 2) # Numero aleatorio para el mapa
		if m == 1:
			self._mapa = Mapa(self._ju, "simple") # Instancia de la clase Mapa (aleatoria)
			print("Mapa: Simple")
		elif m == 2:
			self._mapa = Mapa(self._ju, "forest") # Instancia de la clase Mapa (aleatoria)
			print("Mapa: Forest (Bosque)")
		self._mapa.dibujar() # Dibujo del mapa
		self._basePerro = Base(self._ju, "perro") # Instancia de la clase Base (del perro)
		self._baseGato = Base(self._ju, "gato") # Instancia de la clase Base (del gato)
		self._perro = Jugador(self._ju, "perro") # Instancia de la clase Jugador (perro)
		self._gato = Jugador(self._ju, "gato") # Instancia de la clase Jugador (gato)
		self._items = [] # Lista de items, inicialmente vacia
		self._turno = random.randint(1, 2) # Indicador del turno (aleatorio), 1 para el perro y 2 para el gato
		self._juego = True # Indica si el juego sigue en pie (True) o no (False)

	# juegoEnPie: None -> None
	# Verifica si el juego sigue en pie a traves del campo self._juego, si dicha variable es
	# igual a False el juego se acaba.
	# Ejemplo:
	# Cuerpo del metodo:
	def juegoEnPie(self):
		if self._juego:
			return
		else:
			print("El juego ha terminado") # Mensajes en pantalla avisando el fin del juego
			print("Gracias por jugar")
			sys.exit(0)

	# vueltaBase: instance (jugador) -> None
	# Devuelve al jugador (ingresado como parametro) a una salida aleatoria (y libre) de su base,
	# mostrando un mensaje en pantalla, y entrando a la fase entre-turnos, para luego cambiar el turno.
	# Ejemplo:
	# Cuerpo del metodo:
	def vueltaBase(self, jugador):
		if jugador == self._perro:
			print("Perro ha vuelto a su base") # Mensaje en pantalla que avisa la vuelta del jugador a la base
			salidaBase = self._basePerro.salida()
			if self._mapa.libre(salidaBase[0], salidaBase[1]):
				jugador.mover(salidaBase[0], salidaBase[1])
				self.faseEntreTurnos()
				self.cambioDeTurno()
				return
			else:
				return self.vueltaBase(jugador)
		elif jugador == self._gato:
			print("Gato ha vuelto a su base") # Mensaje en pantalla que avisa la vuelta del jugador a la base
			salidaBase = self._baseGato.salida()
			if self._mapa.libre(salidaBase[0], salidaBase[1]):
				jugador.mover(salidaBase[0], salidaBase[1])
				self.faseEntreTurnos()
				self.cambioDeTurno()
				return
			else:
				return self.vueltaBase(jugador)

	# activarEfecto: instance (item) instance (jugador) -> None
	# Recibe una instancia de las clases objeto y jugador, luego activa el efecto
	# del objeto en dicho jugador, y muestra mensajes en pantalla al respecto.
	# Ejemplo:
	# Cuerpo del metodo:
	def activarEfecto(self, item, jugador):
		if item.getNombre() == "Espada":
			print("Has adquirido la Espada!!!") # Muestra en pantalla el objeto adquirido
			jugador.mejora("A", 20)
			print("Tu ataque ha aumentado en 20 puntos!!!") # Muestra el efecto del objeto
			print("Puntos de ataque: " + str(jugador.getAtaque()))
		elif item.getNombre() == "Escudo":
			print("Has adquirido el Escudo!!!") # Muestra en pantalla el objeto adquirido
			jugador.mejora("D", 15)
			print("Tu defensa ha aumentado en 15 puntos!!!") # Muestra el efecto del objeto
			print("Puntos de defensa: " + str(jugador.getDefensa()))
		elif item.getNombre() == "Pocion":
			print("Has adquirido la Pocion!!!") # Muestra en pantalla el objeto adquirido
			jugador.mejora("H", 50)
			print("Tu salud ha aumentado en 50 puntos!!!") # Muestra el efecto del objeto
			print("Puntos de salud: " + str(int(jugador.getVida())))
		elif item.getNombre() == "Botas":
			print("Has adquirido las Botas!!!") # Muestra en pantalla el objeto adquirido
			jugador.mejora("V", 1)
			print("Tu velocidad ha aumentado en 1 punto!!!") # Muestra el efecto del objeto
			print("Puntos de velocidad: " + str(jugador.getVelocidad()))

	# usarObjetos: instance (jugador) -> None
	# Hace un recorrido en la lista self._items, chequeando si los objetos se encuentran en
	# la posicion del jugador uno por uno, y de ser asi activa el efecto del objeto y lo destruye.
	# Ejemplo:
	# Cuerpo del metodo:
	def usarObjetos(self, jugador):
		for i in range(len(self._items)):
			if ((self._items)[i]).getPosicion() == jugador.getPosicion():
				objeto = ((self._items)[i])
				self.activarEfecto(objeto, jugador)
				objeto.destruir()

	# atacarBase: instance (jugador) -> None
	# Recibe como parametro un jugador y chequea si este esta en la misma posicion que la base enemiga.
	# De ser asi muestra mensajes en pantalla y inicia la secuencia de ataque del jugador a la base, para
	# luego llevar a dicho jugador de vuelta a su base (esto interrumpe el turno del jugador) o terminar el juego.
	# Ejemplo:
	# Cuerpo del metodo:
	def atacarBase(self, jugador):
		if jugador == self._perro:
			base = self._baseGato
			dam = jugador.getAtaque()
			if jugador.getPosicion() == base.getPosicion():
				print("Perro ataca a la base de Gato!!!") # Muestra en pantalla quien ataca a quien
				print("Puntos del ataque: " + str(dam)) # Muestra en pantalla los puntos de dano del ataque
				if base.recibeAtaque(dam):
					self.vueltaBase(jugador) # Si la base sigue con vida el jugador vuelve a su base
				else:
					print("La base ha sido destruida") # En caso contrario avisa en pantalla y se acaba el juego.
					print("Gato ha perdido el juego!")
					print("Felicitaciones Perro!")
					self._juego = False
		elif jugador == self._gato:
			dam = jugador.getAtaque()
			base = self._basePerro
			if jugador.getPosicion() == base.getPosicion():
				print("Gato ataca a la base de Perro!!!") # Muestra en pantalla quien ataca a quien
				print("Puntos del ataque: " + str(dam)) # Muestra en pantalla los puntos de dano del ataque
				if base.recibeAtaque(dam):
					self.vueltaBase(jugador) # Si la base sigue con vida el jugador vuelve a su base
				else:
					print("La base ha sido destruida") # En caso contrario avisa en pantalla y se acaba el juego.
					print("Perro ha perdido el juego!")
					print("Felicitaciones Gato!")
					self._juego = False

	# atacarJugador: instance (jugador) -> None
	# Recibe como parametro un jugador y chequea si este esta en la misma posicion que el jugador enemigo.
	# De ser asi muestra mensajes en pantalla y inicia la secuencia de ataque del jugador al jugador enemigo, para
	# luego llevar al jugador atacante de vuelta a su base (esto interrumpe el turno del jugador) o terminar el juego.
	# Ejemplo:
	# Cuerpo del metodo:
	def atacarJugador(self, jugador):
		r = random.uniform(0.8, 1.2)
		if jugador == self._perro:
			enemigo = self._gato
			a = jugador.getAtaque()
			d = enemigo.getDefensa()
			dam = int(20 * a * r/d)
			if jugador.getPosicion() == enemigo.getPosicion():
				print("Perro ataca a Gato!!!") # Muestra quien ataca a quien
				print("Puntos del ataque: " + str(int(dam))) # Muestra los puntos de dano del ataque
				if enemigo.recibeAtaque(dam): # Si el enemigo sigue con vida
					print("Puntos de vida restantes: " + str(int(enemigo.getVida()))) # Muestra sus puntos de vida
					self.vueltaBase(jugador) # Y lleva al atacante a su base
				else: # En caso contrario:
					print("Puntos de vida restantes: " + str(int(enemigo.getVida()))) # Muestra los puntos de vida del enemigo (0)
					print("Gato ha sido debilitado") # Avisa en pantalla que el jugador perdio y se acaba el juego
					print("Gato ha perdido el juego!")
					print("Felicitaciones Perro!")
					self._juego = False
		elif jugador == self._gato:
			enemigo = self._perro
			a = jugador.getAtaque()
			d = enemigo.getDefensa()
			dam = 20 * a * r/d
			if jugador.getPosicion() == enemigo.getPosicion():
				print("Gato ataca a Perro!!!") # Muestra quien ataca a quien
				dam = jugador.getAtaque()
				print("Puntos del ataque: " + str(int(dam))) # Muestra los puntos de dano del ataque
				if enemigo.recibeAtaque(dam): # Si el enemigo sigue con vida
					print("Puntos de vida restantes: " + str(int(enemigo.getVida()))) # Muestra sus puntos de vida
					self.vueltaBase(jugador) # Y lleva al atacante a su base
				else: # En caso contrario:
					print("Puntos de vida restantes: " + str(int(enemigo.getVida()))) # Muestra los puntos de vida del enemigo (0)
					print("Perro ha sido debilitado") # Avisa en pantalla que el jugador perdio y se acaba el juego
					print("Perro ha perdido el juego!")
					print("Felicitaciones Gato!")
					self._juego = False

	# objetoAleatorio: None -> None
	# Genera un objeto aleatorio en una posicion aleatoria (y libre) del mapa, y luego muestra
	# un mensaje en pantalla.
	# Ejemplo:
	# Cuerpo del metodo:
	def objetoAleatorio(self):
		ju = self._ju
		n = random.randint(1, 4)
		x = random.randint(0, 23)
		y = random.randint(0, 15)
		posicion = [x, y]
		if self._mapa.libre(x, y):
			if n == 1:
				nombre = "Espada"
				item = Item(ju, nombre, x, y)
				print("Ha aparecido un nuevo objeto en el mapa: Espada (Aumenta el ataque)") # Muestra en pantalla que objeto aparecio y su funcion
				self._items += [item]
			elif n == 2:
				nombre = "Escudo"
				item = Item(ju, nombre, x, y)
				print("Ha aparecido un nuevo objeto en el mapa: Escudo (Aumenta la defensa)") # Muestra en pantalla que objeto aparecio y su funcion
				self._items += [item]
			elif n == 3:
				nombre = "Pocion"
				item = Item(ju, nombre, x, y)
				print("Ha aparecido un nuevo objeto en el mapa: Pocion (Aumenta los puntos de vida)") # Muestra en pantalla que objeto aparecio y su funcion
				self._items += [item]
			elif n == 4:
				nombre = "Botas"
				item = Item(ju, nombre, x, y)
				print("Ha aparecido un nuevo objeto en el mapa: Botas (Aumenta la velocidad)") # Muestra en pantalla que objeto aparecio y su funcion
				self._items += [item]
		else:
			self.objetoAleatorio()

	# faseEntreTurnos: None -> None
	# Genera un numero aleatorio r entre 0 y 1, si r es menor a 0.15 genera un objeto aleatorio en el mapa.
	# Ejemplo:
	# Cuerpo del metodo:
	def faseEntreTurnos(self):
		r = random.random()
		if r < 0.15:
			self.objetoAleatorio()

	# cambioDeTurno: None -> None
	# Cambia de turno, cambiando el campo self._turno
	# Ejemplo:
	# Cuerpo del metodo:
	def cambioDeTurno(self):
		if self._turno == 1: # 1 indica que es el turno del perro
			print("Cambio de turno!!!") # Muestra un mensaje en pantalla
			self._turno = 2 # Cambia al turno del otro jugador
			self.cicloJuego() # Y vuelve al ciclo del juego
		elif self._turno == 2: # 2 indica que es el turno del gato
			print("Cambio de turno!!!") # Muestra un mensaje en pantalla
			self._turno = 1 # Cambia al turno del otro jugador
			self.cicloJuego() # Y vuelve al ciclo del juego

	# cicloJuego: None -> None
	# Inicia el juego, muestra mensajes en pantalla en relacion al progreso del juego, 
	# ejecuta el movimiento de los jugadores (interactuando con los usuarios) y realiza los 
	# ataquesa bases, a jugadores, y el consumo de items.
	# Ejemplo:
	# Cuerpo del metodo:
	def cicloJuego(self):
		if self._turno == 1:
			print("Le toca a: Perro (primer jugador)") # Muestra en pantalla a quien le toca jugar
			jugador = self._perro
			r = random.randint(jugador.getVelocidad()-2, jugador.getVelocidad()+2) # Calcula un valor maximo de movimientos
			print("Ingresa un string de a lo mas " + str(r) + " caracteres para moverte") # Pide al usuario un string de a lo mas r caracteres para moverse
			movimiento = input("w (Arriba) - a (Izquerda) - s (Abajo) - d (Derecha) ")
			if type(movimiento) != str: # Chequea que sea string
				print("El dato que ingresaste no es un string.")
				print("Se repetira el turno")
				self.cicloJuego()
			elif len(movimiento) > r: # Chequea que no sea mas largo que r
				print("El string que ingresaste es mas largo de lo que te pedi.")
				print("Se repetira el turno")
				self.cicloJuego()
			for i in range(len(movimiento)): # Chequea que no hayan caracteres que no sean w - a - s - d
				if movimiento[i] != "w" and movimiento[i] != "a" and movimiento[i] != "s" and movimiento[i] != "d":
					print("Los caracteres del string tenian que ser solo w - a - s - d")
					print("Se repetira el turno")
					self.cicloJuego()
			for i in range(len(movimiento)): # Ejecuta el movimiento
				if movimiento[i] == "w":
					nuevaPosicion = [(jugador.getPosicion())[0], (jugador.getPosicion())[1] - 1]
					if self._mapa.libre(nuevaPosicion[0], nuevaPosicion[1]): # Verifica que la posicion este libre
						jugador.mover(nuevaPosicion[0], nuevaPosicion[1]) # Mueve al jugador
						self.usarObjetos(jugador) # Verifica si hay objetos en la posicion para usarlos
						self.atacarJugador(jugador) # Verifica si hay un jugador en la posicion para atacarlo
						self.atacarBase(jugador) # Verifica si hay una base en la posicion para atacarla
					else:
						print("El espacio de tu comando numero " + str(i + 1) + " estaba ocupado.") # Avisa que el espacio estaba ocupado
				elif movimiento[i] == "a":
					nuevaPosicion = [(jugador.getPosicion())[0] - 1, (jugador.getPosicion())[1]]
					if self._mapa.libre(nuevaPosicion[0], nuevaPosicion[1]): # Verifica que la posicion este libre
						jugador.mover(nuevaPosicion[0], nuevaPosicion[1]) # Mueve al jugador
						self.usarObjetos(jugador) # Verifica si hay objetos en la posicion para usarlos
						self.atacarJugador(jugador) # Verifica si hay un jugador en la posicion para atacarlo
						self.atacarBase(jugador) # Verifica si hay una base en la posicion para atacarla
					else:
						print("El espacio de tu comando numero " + str(i + 1) + " estaba ocupado.") # Avisa que el espacio estaba ocupado
				elif movimiento[i] == "s":
					nuevaPosicion = [(jugador.getPosicion())[0], (jugador.getPosicion())[1] + 1]
					if self._mapa.libre(nuevaPosicion[0], nuevaPosicion[1]): # Verifica que la posicion este libre
						jugador.mover(nuevaPosicion[0], nuevaPosicion[1]) # Mueve al jugador
						self.usarObjetos(jugador) # Verifica si hay objetos en la posicion para usarlos
						self.atacarJugador(jugador) # Verifica si hay un jugador en la posicion para atacarlo
						self.atacarBase(jugador) # Verifica si hay una base en la posicion para atacarla
					else:
						print("El espacio de tu comando numero " + str(i + 1) + " estaba ocupado.") # Avisa que el espacio estaba ocupado
				elif movimiento[i] == "d":
					nuevaPosicion = [(jugador.getPosicion())[0] + 1, (jugador.getPosicion())[1]]
					if self._mapa.libre(nuevaPosicion[0], nuevaPosicion[1]): # Verifica que la posicion este libre
						jugador.mover(nuevaPosicion[0], nuevaPosicion[1]) # Mueve al jugador
						self.usarObjetos(jugador) # Verifica si hay objetos en la posicion para usarlos
						self.atacarJugador(jugador) # Verifica si hay un jugador en la posicion para atacarlo
						self.atacarBase(jugador) # Verifica si hay una base en la posicion para atacarla
					else:
						print("El espacio de tu comando numero " + str(i + 1) + " estaba ocupado.") # Avisa que el espacio estaba ocupado
			self.juegoEnPie() # Verifica que el juego siga en pie (en caso contrario se termina el juego)
			self.faseEntreTurnos() # Entra a la fase entre turnos
			self.cambioDeTurno() # Cambia el turno
		elif self._turno == 2:
			print("Le toca a: Gato (segundo jugador)") # Muestra en pantalla a quien le toca jugar
			jugador = self._gato
			r = random.randint(jugador.getVelocidad()-2, jugador.getVelocidad()+2) # Calcula un valor maximo de movimientos
			print("Ingresa un string de a lo mas " + str(r) + " caracteres para moverte") # Pide al usuario un string de a lo mas r caracteres para moverse
			movimiento = input("w (Arriba) - a (Izquerda) - s (Abajo) - d (Derecha) ")
			if type(movimiento) != str: # Chequea que sea string
				print("El dato que ingresaste no es un string.")
				print("Se repetira el turno")
				self.cicloJuego()
			elif len(movimiento) > r: # Chequea que no sea mas largo que r
				print("El string que ingresaste es mas largo de lo que te pedi.")
				print("Se repetira el turno")
				self.cicloJuego()
			for i in range(len(movimiento)): # Chequea que no hayan caracteres que no sean w - a - s - d
				if movimiento[i] != "w" and movimiento[i] != "a" and movimiento[i] != "s" and movimiento[i] != "d":
					print("Los caracteres del string tenian que ser solo w - a - s - d")
					print("Se repetira el turno")
					self.cicloJuego()
			for i in range(len(movimiento)): # Ejecuta el movimiento
				if movimiento[i] == "w":
					nuevaPosicion = [(jugador.getPosicion())[0], (jugador.getPosicion())[1] - 1]
					if self._mapa.libre(nuevaPosicion[0], nuevaPosicion[1]): # Verifica que la posicion este libre
						jugador.mover(nuevaPosicion[0], nuevaPosicion[1]) # Mueve al jugador
						self.usarObjetos(jugador) # Verifica si hay objetos en la posicion para usarlos
						self.atacarJugador(jugador) # Verifica si hay un jugador en la posicion para atacarlo
						self.atacarBase(jugador) # Verifica si hay una base en la posicion para atacarla
					else:
						print("El espacio de tu comando numero " + str(i + 1) + " estaba ocupado.") # Avisa que el espacio estaba ocupado
				elif movimiento[i] == "a":
					nuevaPosicion = [(jugador.getPosicion())[0] - 1, (jugador.getPosicion())[1]]
					if self._mapa.libre(nuevaPosicion[0], nuevaPosicion[1]): # Verifica que la posicion este libre
						jugador.mover(nuevaPosicion[0], nuevaPosicion[1]) # Mueve al jugador
						self.usarObjetos(jugador) # Verifica si hay objetos en la posicion para usarlos
						self.atacarJugador(jugador) # Verifica si hay un jugador en la posicion para atacarlo
						self.atacarBase(jugador) # Verifica si hay una base en la posicion para atacarla
					else:
						print("El espacio de tu comando numero " + str(i + 1) + " estaba ocupado.") # Avisa que el espacio estaba ocupado
				elif movimiento[i] == "s":
					nuevaPosicion = [(jugador.getPosicion())[0], (jugador.getPosicion())[1] + 1]
					if self._mapa.libre(nuevaPosicion[0], nuevaPosicion[1]): # Verifica que la posicion este libre
						jugador.mover(nuevaPosicion[0], nuevaPosicion[1]) # Mueve al jugador
						self.usarObjetos(jugador) # Verifica si hay objetos en la posicion para usarlos
						self.atacarJugador(jugador) # Verifica si hay un jugador en la posicion para atacarlo
						self.atacarBase(jugador) # Verifica si hay una base en la posicion para atacarla
					else:
						print("El espacio de tu comando numero " + str(i + 1) + " estaba ocupado.") # Avisa que el espacio estaba ocupado
				elif movimiento[i] == "d":
					nuevaPosicion = [(jugador.getPosicion())[0] + 1, (jugador.getPosicion())[1]]
					if self._mapa.libre(nuevaPosicion[0], nuevaPosicion[1]): # Verifica que la posicion este libre
						jugador.mover(nuevaPosicion[0], nuevaPosicion[1]) # Mueve al jugador
						self.usarObjetos(jugador) # Verifica si hay objetos en la posicion para usarlos
						self.atacarJugador(jugador) # Verifica si hay un jugador en la posicion para atacarlo
						self.atacarBase(jugador) # Verifica si hay una base en la posicion para atacarla
					else:
						print("El espacio de tu comando numero " + str(i + 1) + " estaba ocupado.") # Avisa que el espacio estaba ocupado
			self.juegoEnPie() # Verifica que el juego siga en pie (en caso contrario se termina el juego)
			self.faseEntreTurnos() # Entra a la fase entre turnos
			self.cambioDeTurno() # Cambia el turno
			
# *Se omite el testeo de esta clase.

Main()