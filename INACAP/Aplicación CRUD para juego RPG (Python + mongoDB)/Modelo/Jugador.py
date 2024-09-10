class Jugador:
    def __init__(self, nombreUsuario, idUsuario, contraseña, correo):
        self.nombreUsuario = nombreUsuario
        self.idUsuario = idUsuario
        self.contraseña = contraseña
        self.correo = correo
        self.cantidadPersonajes = 0

    # Métodos getters y setters

    def getNombreUsuario(self):
        return self.nombreUsuario

    def setNombreUsuario(self, nombreUsuario):
        self.nombreUsuario = nombreUsuario

    def getIdUsuario(self):
        return self.idUsuario

    def setIdUsuario(self, idUsuario):
        self.idUsuario = idUsuario

    def getContraseña(self):
        return self.contraseña

    def setContraseña(self, contraseña):
        self.contraseña = contraseña

    def getCorreo(self):
        return self.correo

    def setCorreo(self, correo):
        self.correo = correo

    def getCantidadPersonajes(self):
        return self.cantidadPersonajes

    def setCantidadPersonajes(self, cantidadPersonajes):
        self.cantidadPersonajes = cantidadPersonajes

    # Otros métodos de la clase Jugador

    def modificarEquipamientoPersonaje(self, personaje):
        # Lógica para modificar el equipamiento del personaje
        pass

    # Otros métodos de la clase Jugador
