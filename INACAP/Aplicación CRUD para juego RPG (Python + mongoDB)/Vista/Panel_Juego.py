import tkinter as tk

class Panel_Juego(tk.Frame):
    def __init__(self, master=None):
        super().__init__(master)
        
        self.atacarButton = tk.Button(self, text="Atacar", command=self.atacar)
        self.defenderButton = tk.Button(self, text="Defender", command=self.defender)
        self.mensajeLabel = tk.Label(self)
        
        self.atacarButton.pack()
        self.defenderButton.pack()
        self.mensajeLabel.pack()

    def atacar(self):
        # Lógica para realizar la acción de ataque en el juego

        self.mensajeLabel["text"] = "¡Has realizado un ataque!"

    def defender(self):
        # Lógica para realizar la acción de defensa en el juego

        self.mensajeLabel["text"] = "¡Te has defendido exitosamente!"
