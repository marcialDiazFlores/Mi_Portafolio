import tkinter as tk

class Panel_Jugador(tk.Frame):
    def __init__(self, master=None):
        super().__init__(master)
        
        self.modificarEquipamientoButton = tk.Button(self, text="Modificar Equipamiento", command=self.modificarEquipamiento)
        self.mensajeLabel = tk.Label(self)
        
        self.modificarEquipamientoButton.pack()
        self.mensajeLabel.pack()

    def modificarEquipamiento(self):
        # Lógica para modificar el equipamiento del personaje

        self.mensajeLabel["text"] = "¡Se ha modificado el equipamiento!"
