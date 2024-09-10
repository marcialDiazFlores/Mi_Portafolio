class Juego:
    def __init__(self):
        self.tablaResumen = []
        self.listadoHabilidades = []

    # Métodos getters y setters

    def getTablaResumen(self):
        return self.tablaResumen

    def setTablaResumen(self, tablaResumen):
        self.tablaResumen = tablaResumen

    def getListadoHabilidades(self):
        return self.listadoHabilidades

    def setListadoHabilidades(self, listadoHabilidades):
        self.listadoHabilidades = listadoHabilidades

    # Otros métodos de la clase Juego

    def asignarVerificarCorreo(self, correo):
        # Lógica para asignar y verificar el correo
        pass

    def agregarPersonaje(self, personaje):
        self.tablaResumen.append(personaje)

    def agregarHabilidad(self, habilidad):
        self.listadoHabilidades.append(habilidad)

    # Otros métodos de la clase Juego
