class Persona:
    def __init__(self, nombre, rut, apellido, id = None):
        self.id = id 
        self.nombre = nombre
        self.rut = rut
        self.apellido = apellido

    def mostrar_info(self):
        print(f"Persona: {self.nombre} {self.apellido} | Rut: {self.rut}")
