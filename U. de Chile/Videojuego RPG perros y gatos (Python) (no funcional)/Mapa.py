import pygame

class Mapa:
    def __init__(self, ventana, nom):
        assert type(nom) == str
        self._ventana = ventana  # Instancia de la ventana de Pygame
        self._mapa = self.cargarMapa(nom)  # Matriz del mapa cargado

    def cargarMapa(self, nom):
        mapa = []  # Matriz para almacenar el mapa

        # Abre el archivo de mapa
        with open(nom + ".txt", "r") as archivo_mapa:
            for linea in archivo_mapa:
                fila = linea.strip()  # Elimina espacios en blanco y saltos de línea
                mapa.append(list(fila))  # Convierte la fila en una lista de caracteres

        return mapa

    def dibujar(self):
        for y in range(len(self._mapa)):
            filaI = self._mapa[y]
            for x in range(len(filaI)):
                celda = filaI[x]
                if celda == "0":
                    self._ventana.blit(grass_image, (x*32, y*32))
                elif celda == "1":
                    self._ventana.blit(ground_image, (x*32, y*32))
                elif celda == "2":
                    self._ventana.blit(trees_image, (x*32, y*32))

    def libre(self, x, y):
        assert type(x) == int and type(y) == int
        assert x >= 0 and x <= 24
        assert y >= 0 and y <= 16
        if self._mapa[y][x] == "0" or self._mapa[y][x] == "1" or self._mapa[y][x] == "5":
            return True
        else:
            return False




# Clase Mapa
# Campos:
# ju (instance)
# mapa (list)

"""

class Mapa:
	# Constructor:
	def __init__(self, ju, nom):
		assert type(nom) == str
		# Inicializacion de campos:
		self._ju = ju # Instancia de la clase JUtil
		self._mapa = self._ju.cargarMapa(nom) # Matriz del mapa cargado

    # dibujar: None -> None
    # Dibuja el mapa en una ventana creada previamente.
    # Cuerpo del metodo:
	def dibujar(self):
		for y in range(len(self._mapa)):
			filaI = (self._mapa)[y]
			for x in range(len(filaI)):
				celda = filaI[x]
				if celda == "0":
					self._ju.dibujarEstatico("grass", x*32, y*32)
				elif celda == "1":
					self._ju.dibujarEstatico("ground", x*32, y*32)
				elif celda =="2":
					self._ju.dibujarEstatico("trees", x*32, y*32)
    
    # libre: int int -> bool
    # Recibe dos parametros de posicion (en los ejes x e y) y verifica que dicha posicion
    # este libre en el mapa, retornando True en dicho caso, y False en caso contrario.
    # Ejemplos: Para ju = JUtil() y simple = Mapa(ju, "simple"), simple.libre(3, 4) retorna
    # True y simple.libre(0, 1) retorna False.
    # Cuerpo del metodo:
	def libre(self, x, y):
		assert type(x) == int and type(y) == int
		assert x >= 0 and x <= 24
		assert y >= 0 and y <= 16
		if (self._mapa)[y][x] == "0" or (self._mapa)[y][x] == "1" or (self._mapa)[y][x] == "5":
			return True
		else:
			return False
		
"""
			
# *Los tests de los metodos de esta clase se encuentran en un modulo aparte.