class Personaje:
    def __init__(self, nivel, nombre, edad, sexo, raza):
        self.nivel = nivel
        self.nombre = nombre
        self.edad = edad
        self.sexo = sexo
        self.raza = raza
        self.habilidades = None
        self.poder = ""
        self.equipamiento = ""
        self.estado = ""
        self.estadisticas = None

    # Métodos getters y setters

    def getNivel(self):
        return self.nivel

    def setNivel(self, nivel):
        self.nivel = nivel

    def getNombre(self):
        return self.nombre

    def setNombre(self, nombre):
        self.nombre = nombre

    def getEdad(self):
        return self.edad

    def setEdad(self, edad):
        self.edad = edad

    def getSexo(self):
        return self.sexo

    def setSexo(self, sexo):
        self.sexo = sexo

    def getRaza(self):
        return self.raza

    def setRaza(self, raza):
        self.raza = raza

    def getHabilidades(self):
        return self.habilidades

    def setHabilidades(self, habilidades):
        self.habilidades = habilidades

    def getPoder(self):
        return self.poder

    def setPoder(self, poder):
        self.poder = poder

    def getEquipamiento(self):
        return self.equipamiento

    def setEquipamiento(self, equipamiento):
        self.equipamiento = equipamiento

    def getEstado(self):
        return self.estado

    def setEstado(self, estado):
        self.estado = estado

    def getEstadisticas(self):
        return self.estadisticas

    def setEstadisticas(self, estadisticas):
        self.estadisticas = estadisticas

    # Métodos de la clase Personaje

    def atacar(self):
        # Lógica para realizar un ataque
        pass

    def defender(self):
        # Lógica para realizar una defensa
        pass

    def usarEquipamiento(self):
        # Lógica para usar el equipamiento
        pass

    def usarPoder(self):
        # Lógica para usar el poder
        pass

    def usarHabilidad(self):
        # Lógica para usar la habilidad
        pass
