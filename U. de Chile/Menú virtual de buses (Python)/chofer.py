from persona import Persona

class Chofer(Persona):
    def __init__(self, rut="", nombre="", antiguedad=0):
        super().__init__(rut, nombre)
        self.__antiguedad = antiguedad

    def __str__(self):
        return super().__str__() + f" Antiguedad: {self.__antiguedad}"
    
    def getAntiguedad(self):
        return self.__antiguedad

    def setAntiguedad(self, antiguedad):
        self.__antiguedad = antiguedad

    