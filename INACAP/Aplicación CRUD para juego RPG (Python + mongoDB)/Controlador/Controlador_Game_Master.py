class Controlador_Game_Master:
    def __init__(self):
        self.gameMasters = []

    def agregarGameMaster(self, gameMaster):
        self.gameMasters.append(gameMaster)

    def eliminarGameMaster(self, gameMaster):
        self.gameMasters.remove(gameMaster)

    def obtenerGameMasters(self):
        return self.gameMasters
