class Controlador_Juego:
    def __init__(self):
        self.juegos = []

    def agregarJuego(self, juego):
        self.juegos.append(juego)

    def eliminarJuego(self, juego):
        self.juegos.remove(juego)

    def obtenerJuegos(self):
        return self.juegos
