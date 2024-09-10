class GameMaster:
    def __init__(self, nombreUsuario, idUsuario, contraseña, correo):
        self.nombreUsuario = nombreUsuario
        self.idUsuario = idUsuario
        self.contraseña = contraseña
        self.correo = correo

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

    # Otros métodos de la clase GameMaster

    def ingresarHabilidadesPersonaje(self, personaje):
        # Lógica para ingresar habilidades al personaje
        pass

    def editarHabilidadesPersonaje(self, personaje):
        # Lógica para editar habilidades del personaje
        pass

    def ingresarEquipamientoPersonaje(self, personaje):
        # Lógica para ingresar equipamiento al personaje
        pass

    def agregarPoderesPersonaje(self, personaje):
        # Lógica para agregar poderes al personaje
        pass

    def reemplazarPoderesPersonaje(self, personaje):
        # Lógica para reemplazar poderes del personaje
        pass

    def subirNivelPersonaje(self, personaje):
        # Lógica para subir el nivel del personaje
        pass

    def cambiarEstadoPersonaje(self, personaje, nuevoEstado):
        # Lógica para cambiar el estado del personaje
        pass

    def eliminarEstadoPersonaje(self, estado):
        # Lógica para eliminar un estado del juego
        pass

    def eliminarRazaPersonaje(self, raza):
        # Lógica para eliminar una raza del juego
        pass

    def eliminarHabilidadPersonaje(self, habilidad):
        # Lógica para eliminar una habilidad del juego
        pass

    def eliminarPoderPersonaje(self, poder):
        # Lógica para eliminar un poder del juego
        pass

    # Otros métodos de la clase GameMaster
