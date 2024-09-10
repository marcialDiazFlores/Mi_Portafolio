from persona import Persona

class Pasajero(Persona):
    def __init__(self, rut, nombre, edad):
        super().__init__(rut, nombre)
        self.__edad = edad

    def __str__(self):
        return super().__str__() + f" Edad: {self.__edad}"

    def getEdad(self):
        return self.__edad

    def setEdad(self, edad):
        return self.__edad