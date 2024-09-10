import tkinter as tk

class Panel_Personaje(tk.Frame):
    def __init__(self, master=None):
        super().__init__(master)
        
        self.atacarButton = tk.Button(self, text="Atacar", command=self.atacar)
        self.defenderButton = tk.Button(self, text="Defender", command=self.defender)
        self.usarEquipamientoButton = tk.Button(self, text="Usar Equipamiento", command=self.usarEquipamiento)
        self.usarPoderButton = tk.Button(self, text="Usar Poder", command=self.usarPoder)
        self.usarHabilidadButton = tk.Button(self, text="Usar Habilidad", command=self.usarHabilidad)
        self.mensajeLabel = tk.Label(self)
        
        self.atacarButton.pack()
        self.defenderButton.pack()
        self.usarEquipamientoButton.pack()
        self.usarPoderButton.pack()
        self.usarHabilidadButton.pack()
        self.mensajeLabel.pack()

    def atacar(self):
        # Lógica para realizar la acción de atacar

        self.mensajeLabel["text"] = "¡Has realizado un ataque!"

    def defender(self):
        # Lógica para realizar la acción de defender

        self.mensajeLabel["text"] = "¡Te has defendido!"

    def usarEquipamiento(self):
        # Lógica para realizar la acción de usar equipamiento

        self.mensajeLabel["text"] = "¡Has usado un equipamiento!"

    def usarPoder(self):
        # Lógica para realizar la acción de usar poder

        self.mensajeLabel["text"] = "¡Has usado un poder!"

    def usarHabilidad(self):
        # Lógica para realizar la acción de usar habilidad

        self.mensajeLabel["text"] = "¡Has usado una habilidad!"
