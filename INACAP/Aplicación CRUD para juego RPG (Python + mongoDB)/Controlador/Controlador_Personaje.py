class Controlador_Personaje:
    def __init__(self):
        self.personajes = []

    def agregarPersonaje(self, personaje):
        self.personajes.append(personaje)

    def eliminarPersonaje(self, personaje):
        self.personajes.remove(personaje)

    def obtenerPersonajes(self):
        return self.personajes
