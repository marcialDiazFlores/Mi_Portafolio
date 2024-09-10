class Controlador_Jugador:
    def __init__(self):
        self.jugadores = []

    def agregarJugador(self, jugador):
        self.jugadores.append(jugador)

    def eliminarJugador(self, jugador):
        self.jugadores.remove(jugador)

    def obtenerJugadores(self):
        return self.jugadores
