class Disco:
    def __init__(self, titulo, artista, lanzamiento, disponible=True, id=None):
        self.id = id 
        self.titulo = titulo
        self.artista = artista
        self.lanzamiento = lanzamiento
        self.disponible = disponible

    def mostrar_info(self):
        if self.disponible:
            print(f"Disco: {self.titulo} | Artista: {self.artista} | Año de lanzamiento: {self.lanzamiento} | Disponible: Sí")
        else:
            print(f"Disco: {self.titulo} | Artista: {self.artista} | Año de lanzamiento: {self.lanzamiento} | Disponible: No")