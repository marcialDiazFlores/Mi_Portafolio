import tkinter as tk

class Panel_Game_Master(tk.Frame):
    def __init__(self, master=None):
        super().__init__(master)
        
        self.ingresarHabilidadButton = tk.Button(self, text="Ingresar Habilidad", command=self.ingresarHabilidad)
        self.editarHabilidadButton = tk.Button(self, text="Editar Habilidad", command=self.editarHabilidad)
        self.nombreHabilidadField = tk.Entry(self)
        self.mensajeLabel = tk.Label(self)
        
        self.nombreHabilidadField.pack()
        self.ingresarHabilidadButton.pack()
        self.editarHabilidadButton.pack()
        self.mensajeLabel.pack()

    def ingresarHabilidad(self):
        nombreHabilidad = self.nombreHabilidadField.get()
        # Lógica para ingresar la habilidad del personaje

        self.mensajeLabel["text"] = f"Habilidad ingresada: {nombreHabilidad}"

    def editarHabilidad(self):
        nombreHabilidad = self.nombreHabilidadField.get()
        # Lógica para editar la habilidad del personaje

        self.mensajeLabel["text"] = f"Habilidad editada: {nombreHabilidad}"
