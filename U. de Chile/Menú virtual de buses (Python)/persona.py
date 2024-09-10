class Persona:
    def __init__(self, rut="", nombre=""):
        self.__rut = rut
        self.__nombre = nombre

    def __str__(self):
        return f"Rut: {self.__rut} Nombre: {self.__nombre}"

    def getRut(self):
        return self.__rut

    def setRut(self, rut):
        self.__rut = rut
