import tkinter as tk
from tkinter import messagebox
from pymongo import MongoClient

# Conexión a la base de datos MongoDB
client = MongoClient("mongodb://localhost:27017")
db = client["RPG"]
jugadores_collection = db["jugadores"]
game_masters_collection = db["game_masters"]
tabla_resumen_collection = db["tabla_resumen"]
listado_habilidades_collection = db["listado_habilidades"]
personajes_collection = db["personajes"]

class ActualizarJugadorInterface(tk.Toplevel):
    def __init__(self, parent, jugador):
        super().__init__(parent)
        self.title("Actualizar Jugador")
        self.geometry("300x250")

        self.jugador = jugador

        self.nombre_usuario_label = tk.Label(self, text="Nombre de Usuario:")
        self.nombre_usuario_label.pack()
        self.nombre_usuario_entry = tk.Entry(self)
        self.nombre_usuario_entry.insert(0, jugador["nombre_usuario"])
        self.nombre_usuario_entry.pack()

        self.contrasena_label = tk.Label(self, text="Contraseña:")
        self.contrasena_label.pack()
        self.contrasena_entry = tk.Entry(self, show="*")
        self.contrasena_entry.insert(0, jugador["contrasena"])
        self.contrasena_entry.pack()

        self.correo_label = tk.Label(self, text="Correo:")
        self.correo_label.pack()
        self.correo_entry = tk.Entry(self)
        self.correo_entry.insert(0, jugador["correo"])
        self.correo_entry.pack()

        self.cantidad_personajes_label = tk.Label(self, text="Cantidad de Personajes:")
        self.cantidad_personajes_label.pack()
        self.cantidad_personajes_entry = tk.Entry(self)
        self.cantidad_personajes_entry.insert(0, str(jugador["cantidad_personajes"]))
        self.cantidad_personajes_entry.pack()

        self.actualizar_button = tk.Button(self, text="Actualizar", command=self.actualizar_jugador)
        self.actualizar_button.pack()

    def actualizar_jugador(self):
        nombre_usuario = self.nombre_usuario_entry.get()
        contrasena = self.contrasena_entry.get()
        correo = self.correo_entry.get()
        cantidad_personajes = int(self.cantidad_personajes_entry.get())

        # Validación de los campos
        if not (4 <= len(nombre_usuario) <= 20):
            messagebox.showerror("Error", "Nombre de usuario inválido. Ingrese un string de largo entre 4 y 20 caracteres.")
            return

        if not (3 <= len(contrasena) <= 20):
            messagebox.showerror("Error", "Contraseña inválida. Ingrese un string de largo entre 3 y 20 caracteres.")
            return

        if not (4 <= len(correo) <= 50):
            messagebox.showerror("Error", "Correo inválido. Ingrese un string de largo entre 4 y 50 caracteres.")
            return

        if not (1 <= cantidad_personajes <= 4):
            messagebox.showerror("Error", "Cantidad de personajes inválida. Ingrese un entero positivo entre 1 y 4.")
            return

        # Actualizar los datos en la base de datos
        jugadores_collection.update_one({"_id": self.jugador["_id"]}, {"$set": {
            "nombre_usuario": nombre_usuario,
            "contrasena": contrasena,
            "correo": correo,
            "cantidad_personajes": cantidad_personajes
        }})

        messagebox.showinfo("Éxito", "Jugador actualizado correctamente.")
        self.destroy()


# Interfaz para la colección Jugadores
class JugadoresInterface(tk.Toplevel):
    def __init__(self, parent):
        super().__init__(parent)
        self.title("Jugadores")
        self.geometry("300x30")

        self.opciones_frame = tk.Frame(self)
        self.opciones_frame.pack()

        self.crear_button = tk.Button(self.opciones_frame, text="Crear", command=self.mostrar_formulario_crear)
        self.crear_button.pack(side=tk.LEFT)

        self.buscar_button = tk.Button(self.opciones_frame, text="Buscar", command=self.mostrar_formulario_buscar)
        self.buscar_button.pack(side=tk.LEFT)

        self.actualizar_button = tk.Button(self.opciones_frame, text="Actualizar", command=self.mostrar_formulario_actualizar)
        self.actualizar_button.pack(side=tk.LEFT)

        self.eliminar_button = tk.Button(self.opciones_frame, text="Eliminar", command=self.mostrar_formulario_eliminar)
        self.eliminar_button.pack(side=tk.LEFT)


    def mostrar_formulario_crear(self):
        # Crear una nueva ventana para el formulario de creación
        formulario_crear = tk.Toplevel(self)
        formulario_crear.title("Crear Jugador")
        formulario_crear.geometry("300x250")

        # Campos del formulario
        nombre_usuario_label = tk.Label(formulario_crear, text="Nombre de Usuario:")
        nombre_usuario_label.pack()
        nombre_usuario_entry = tk.Entry(formulario_crear)
        nombre_usuario_entry.pack()

        id_usuario_label = tk.Label(formulario_crear, text="ID de Usuario:")
        id_usuario_label.pack()
        id_usuario_entry = tk.Entry(formulario_crear)
        id_usuario_entry.pack()

        contrasena_label = tk.Label(formulario_crear, text="Contraseña:")
        contrasena_label.pack()
        contrasena_entry = tk.Entry(formulario_crear, show="*")
        contrasena_entry.pack()

        correo_label = tk.Label(formulario_crear, text="Correo:")
        correo_label.pack()
        correo_entry = tk.Entry(formulario_crear)
        correo_entry.pack()

        cantidad_personajes_label = tk.Label(formulario_crear, text="Cantidad de Personajes:")
        cantidad_personajes_label.pack()
        cantidad_personajes_entry = tk.Entry(formulario_crear)
        cantidad_personajes_entry.pack()

        # Función para guardar los datos del jugador en la base de datos
        def guardar_jugador():
            nombre_usuario = nombre_usuario_entry.get()
            id_usuario = id_usuario_entry.get()
            contrasena = contrasena_entry.get()
            correo = correo_entry.get()
            cantidad_personajes = cantidad_personajes_entry.get()

            # Validación de los campos

            if not (4 <= len(nombre_usuario) <= 20):
                messagebox.showerror("Error", "Nombre de usuario inválido. Ingrese un string de largo entre 4 y 20 caracteres.")
                return

            if not (4 <= len(id_usuario) <= 20):
                messagebox.showerror("Error", "ID de usuario inválido. Ingrese un string de largo entre 4 y 20 caracteres.")
                return

            if not (3 <= len(contrasena) <= 20):
                messagebox.showerror("Error", "Contraseña inválida. Ingrese un string de largo entre 3 y 20 caracteres.")
                return

            if not (4 <= len(correo) <= 50):
                messagebox.showerror("Error", "Correo inválido. Ingrese un string de largo entre 4 y 50 caracteres.")
                return

            try:
                cantidad_personajes = int(cantidad_personajes)
                if not (1 <= cantidad_personajes <= 4):
                    raise ValueError
            except ValueError:
                messagebox.showerror("Error", "Cantidad de personajes inválida. Ingrese un entero positivo entre 1 y 4.")
                return

            # Crear el jugador en la base de datos
            jugador = {
                "nombre_usuario": nombre_usuario,
                "id_usuario": id_usuario,
                "contrasena": contrasena,
                "correo": correo,
                "cantidad_personajes": cantidad_personajes
            }

            jugadores_collection.insert_one(jugador)
            messagebox.showinfo("Éxito", "Jugador guardado correctamente.")

            # Cerrar la ventana del formulario después de guardar los datos
            formulario_crear.destroy()

        # Botón para guardar los datos
        guardar_button = tk.Button(formulario_crear, text="Guardar", command=guardar_jugador)
        guardar_button.pack()

    def mostrar_formulario_buscar(self):

        # Crear una nueva ventana para el formulario de búsqueda
        formulario_buscar = tk.Toplevel(self)
        formulario_buscar.title("Buscar Jugador")
        formulario_buscar.geometry("300x150")

        # Campo de entrada para el ID de usuario
        id_usuario_label = tk.Label(formulario_buscar, text="ID de Usuario:")
        id_usuario_label.pack()
        id_usuario_entry = tk.Entry(formulario_buscar)
        id_usuario_entry.pack()

        # Función para buscar el jugador en la base de datos
        def buscar_jugador():
            id_usuario = id_usuario_entry.get()

            # Validación del campo ID de usuario
            if not (4 <= len(id_usuario) <= 20):
                messagebox.showerror("Error", "ID de usuario inválido. Ingrese un string de largo entre 4 y 20 caracteres.")
                return

            # Buscar el jugador en la base de datos
            jugador = jugadores_collection.find_one({"id_usuario": id_usuario})

            if jugador:
                # Mostrar los atributos del jugador en una nueva ventana
                ventana_jugador = tk.Toplevel(formulario_buscar)
                ventana_jugador.title("Jugador Encontrado")
                ventana_jugador.geometry("300x200")

                # Mostrar los atributos del jugador
                nombre_usuario_label = tk.Label(ventana_jugador, text=f"Nombre de Usuario: {jugador['nombre_usuario']}")
                nombre_usuario_label.pack()

                id_usuario_label = tk.Label(ventana_jugador, text=f"ID de Usuario: {jugador['id_usuario']}")
                id_usuario_label.pack()

                contrasena_label = tk.Label(ventana_jugador, text=f"Contraseña: {jugador['contrasena']}")
                contrasena_label.pack()

                correo_label = tk.Label(ventana_jugador, text=f"Correo: {jugador['correo']}")
                correo_label.pack()

                cantidad_personajes_label = tk.Label(ventana_jugador, text=f"Cantidad de Personajes: {jugador['cantidad_personajes']}")
                cantidad_personajes_label.pack()
            else:
                messagebox.showerror("Error", "Jugador no encontrado.")

        # Botón para buscar el jugador
        buscar_button = tk.Button(formulario_buscar, text="Buscar", command=buscar_jugador)
        buscar_button.pack()

    def mostrar_formulario_actualizar(self):
        # Crear una nueva ventana para el formulario de actualización
        formulario_actualizar = tk.Toplevel(self)
        formulario_actualizar.title("Actualizar Jugador")
        formulario_actualizar.geometry("300x200")

        # Campo de entrada para el ID de usuario
        id_usuario_label = tk.Label(formulario_actualizar, text="ID de Usuario:")
        id_usuario_label.pack()
        id_usuario_entry = tk.Entry(formulario_actualizar)
        id_usuario_entry.pack()

        # Función para buscar el jugador en la base de datos y mostrar el formulario de actualización
        def buscar_jugador_actualizar():
            id_usuario = id_usuario_entry.get()

            # Validación del campo ID de usuario
            if not (4 <= len(id_usuario) <= 20):
                messagebox.showerror("Error", "ID de usuario inválido. Ingrese un string de largo entre 4 y 20 caracteres.")
                return

            # Buscar el jugador en la base de datos
            jugador = jugadores_collection.find_one({"id_usuario": id_usuario})

            if jugador:
                # Cerrar la ventana de búsqueda
                formulario_actualizar.destroy()

                # Mostrar una nueva ventana con el formulario de actualización
                ventana_actualizar = tk.Toplevel(self)
                ventana_actualizar.title("Actualizar Jugador")
                ventana_actualizar.geometry("300x200")

                # Campos de entrada para los atributos actualizados
                nuevo_nombre_usuario_label = tk.Label(ventana_actualizar, text="Nuevo Nombre de Usuario:")
                nuevo_nombre_usuario_label.pack()
                nuevo_nombre_usuario_entry = tk.Entry(ventana_actualizar)
                nuevo_nombre_usuario_entry.pack()

                nueva_contrasena_label = tk.Label(ventana_actualizar, text="Nueva Contraseña:")
                nueva_contrasena_label.pack()
                nueva_contrasena_entry = tk.Entry(ventana_actualizar, show="*")
                nueva_contrasena_entry.pack()

                nuevo_correo_label = tk.Label(ventana_actualizar, text="Nuevo Correo:")
                nuevo_correo_label.pack()
                nuevo_correo_entry = tk.Entry(ventana_actualizar)
                nuevo_correo_entry.pack()

                nueva_cantidad_personajes_label = tk.Label(ventana_actualizar, text="Nueva Cantidad de Personajes:")
                nueva_cantidad_personajes_label.pack()
                nueva_cantidad_personajes_entry = tk.Entry(ventana_actualizar)
                nueva_cantidad_personajes_entry.pack()

                # Función para actualizar los atributos en la base de datos
                def actualizar_jugador():
                    nuevo_nombre_usuario = nuevo_nombre_usuario_entry.get()
                    nueva_contrasena = nueva_contrasena_entry.get()
                    nuevo_correo = nuevo_correo_entry.get()
                    nueva_cantidad_personajes = nueva_cantidad_personajes_entry.get()

                    if not (4 <= len(nuevo_nombre_usuario) <= 20):
                        messagebox.showerror("Error", "Nombre de usuario inválido. Ingrese un string de largo entre 4 y 20 caracteres.")
                        return

                    if not (3 <= len(nueva_contrasena) <= 20):
                        messagebox.showerror("Error", "Contraseña inválida. Ingrese un string de largo entre 3 y 20 caracteres.")
                        return

                    if not (4 <= len(nuevo_correo) <= 50):
                        messagebox.showerror("Error", "Correo inválido. Ingrese un string de largo entre 4 y 50 caracteres.")
                        return

                    try:
                        nueva_cantidad_personajes = int(nueva_cantidad_personajes)
                        if not (1 <= nueva_cantidad_personajes <= 4):
                            raise ValueError
                    except ValueError:
                        messagebox.showerror("Error", "Cantidad de personajes inválida. Ingrese un entero positivo entre 1 y 4.")
                        return

                    # Actualizar los datos en la base de datos
                    jugadores_collection.update_one({"id_usuario": id_usuario}, {"$set": {
                        "nombre_usuario": nuevo_nombre_usuario,
                        "contrasena": nueva_contrasena,
                        "correo": nuevo_correo,
                        "cantidad_personajes": nueva_cantidad_personajes
                    }})

                    messagebox.showinfo("Éxito", "Jugador actualizado correctamente.")
                    ventana_actualizar.destroy()

                # Botón para actualizar el jugador
                actualizar_button = tk.Button(ventana_actualizar, text="Actualizar", command=actualizar_jugador)
                actualizar_button.pack()
            else:
                messagebox.showerror("Error", "Jugador no encontrado.")

        # Botón para buscar el jugador y mostrar el formulario de actualización
        buscar_button = tk.Button(formulario_actualizar, text="Buscar", command=buscar_jugador_actualizar)
        buscar_button.pack()

    def mostrar_formulario_eliminar(self):
        # Crear una nueva ventana para el formulario de eliminación
        formulario_eliminar = tk.Toplevel(self)
        formulario_eliminar.title("Eliminar Jugador")
        formulario_eliminar.geometry("300x150")

        # Campo de entrada para el ID de usuario
        id_usuario_label = tk.Label(formulario_eliminar, text="ID de Usuario:")
        id_usuario_label.pack()
        id_usuario_entry = tk.Entry(formulario_eliminar)
        id_usuario_entry.pack()

        # Función para buscar el jugador en la base de datos y eliminarlo
        def buscar_jugador_eliminar():
            id_usuario = id_usuario_entry.get()

            # Validación del campo ID de usuario
            if not (4 <= len(id_usuario) <= 20):
                messagebox.showerror("Error", "ID de usuario inválido. Ingrese un string de largo entre 4 y 20 caracteres.")
                return

            # Buscar el jugador en la base de datos
            jugador = jugadores_collection.find_one({"id_usuario": id_usuario})

            if jugador:
                # Eliminar el jugador de la base de datos
                jugadores_collection.delete_one({"id_usuario": id_usuario})
                messagebox.showinfo("Éxito", "Jugador eliminado correctamente.")
                formulario_eliminar.destroy()
            else:
                messagebox.showerror("Error", "Jugador no encontrado.")

        # Botón para buscar el jugador y eliminarlo
        buscar_button = tk.Button(formulario_eliminar, text="Buscar", command=buscar_jugador_eliminar)
        buscar_button.pack()

class GameMastersInterface(tk.Toplevel):
    def __init__(self, parent):
        super().__init__(parent)
        self.title("Game Masters")
        self.geometry("300x200")

        self.opciones_frame = tk.Frame(self)
        self.opciones_frame.pack()

        self.crear_button = tk.Button(self.opciones_frame, text="Crear", command=self.mostrar_formulario_crear)
        self.crear_button.pack(side=tk.LEFT)

        self.buscar_button = tk.Button(self.opciones_frame, text="Buscar", command=self.mostrar_formulario_buscar)
        self.buscar_button.pack(side=tk.LEFT)

        self.actualizar_button = tk.Button(self.opciones_frame, text="Actualizar", command=self.mostrar_formulario_actualizar)
        self.actualizar_button.pack(side=tk.LEFT)

        self.eliminar_button = tk.Button(self.opciones_frame, text="Eliminar", command=self.mostrar_formulario_eliminar)
        self.eliminar_button.pack(side=tk.LEFT)

    def mostrar_formulario_crear(self):
        # Crear una nueva ventana para el formulario de creación
        formulario_crear = tk.Toplevel(self)
        formulario_crear.title("Crear Game Master")
        formulario_crear.geometry("300x250")

        # Campos del formulario
        nombre_usuario_label = tk.Label(formulario_crear, text="Nombre de Usuario:")
        nombre_usuario_label.pack()
        nombre_usuario_entry = tk.Entry(formulario_crear)
        nombre_usuario_entry.pack()

        id_usuario_label = tk.Label(formulario_crear, text="ID de Usuario:")
        id_usuario_label.pack()
        id_usuario_entry = tk.Entry(formulario_crear)
        id_usuario_entry.pack()

        contrasena_label = tk.Label(formulario_crear, text="Contraseña:")
        contrasena_label.pack()
        contrasena_entry = tk.Entry(formulario_crear, show="*")
        contrasena_entry.pack()

        correo_label = tk.Label(formulario_crear, text="Correo:")
        correo_label.pack()
        correo_entry = tk.Entry(formulario_crear)
        correo_entry.pack()

        # Función para guardar los datos del game master en la base de datos
        def guardar_game_master():
            nombre_usuario = nombre_usuario_entry.get()
            id_usuario = id_usuario_entry.get()
            contrasena = contrasena_entry.get()
            correo = correo_entry.get()

            # Validación de los campos
            if not (4 <= len(nombre_usuario) <= 20):
                messagebox.showerror("Error", "Nombre de usuario inválido. Ingrese un string de largo entre 4 y 20 caracteres.")
                return

            if not (4 <= len(id_usuario) <= 20):
                messagebox.showerror("Error", "ID de usuario inválido. Ingrese un string de largo entre 4 y 20 caracteres.")
                return

            if not (3 <= len(contrasena) <= 20):
                messagebox.showerror("Error", "Contraseña inválida. Ingrese un string de largo entre 3 y 20 caracteres.")
                return

            if not (4 <= len(correo) <= 50):
                messagebox.showerror("Error", "Correo inválido. Ingrese un string de largo entre 4 y 50 caracteres.")
                return

            # Crear al game master en la base de datos
            game_master = {
                "nombre_usuario": nombre_usuario,
                "id_usuario": id_usuario,
                "contrasena": contrasena,
                "correo": correo
            }

            game_masters_collection.insert_one(game_master)
            messagebox.showinfo("Éxito", "Game Master guardado correctamente.")

            # Cerrar la ventana del formulario después de guardar los datos
            formulario_crear.destroy()

        # Botón para guardar los datos
        guardar_button = tk.Button(formulario_crear, text="Guardar", command=guardar_game_master)
        guardar_button.pack()

    def mostrar_formulario_buscar(self):

        # Crear una nueva ventana para el formulario de búsqueda
        formulario_buscar = tk.Toplevel(self)
        formulario_buscar.title("Buscar Game Master")
        formulario_buscar.geometry("300x150")

        # Campo de entrada para el ID de usuario
        id_usuario_label = tk.Label(formulario_buscar, text="ID de Usuario:")
        id_usuario_label.pack()
        id_usuario_entry = tk.Entry(formulario_buscar)
        id_usuario_entry.pack()

        # Función para buscar el jugador en la base de datos
        def buscar_game_master():
            id_usuario = id_usuario_entry.get()

            # Validación del campo ID de usuario
            if not (4 <= len(id_usuario) <= 20):
                messagebox.showerror("Error", "ID de usuario inválido. Ingrese un string de largo entre 4 y 20 caracteres.")
                return

            # Buscar al game master en la base de datos
            game_master = game_masters_collection.find_one({"id_usuario": id_usuario})

            if game_master:
                # Mostrar los atributos del game master en una nueva ventana
                ventana_game_master = tk.Toplevel(formulario_buscar)
                ventana_game_master.title("Jugador Encontrado")
                ventana_game_master.geometry("300x200")

                # Mostrar los atributos del jugador
                nombre_usuario_label = tk.Label(ventana_game_master, text=f"Nombre de Usuario: {game_master['nombre_usuario']}")
                nombre_usuario_label.pack()

                id_usuario_label = tk.Label(ventana_game_master, text=f"ID de Usuario: {game_master['id_usuario']}")
                id_usuario_label.pack()

                contrasena_label = tk.Label(ventana_game_master, text=f"Contraseña: {game_master['contrasena']}")
                contrasena_label.pack()

                correo_label = tk.Label(ventana_game_master, text=f"Correo: {game_master['correo']}")
                correo_label.pack()

            else:
                messagebox.showerror("Error", "Game Master no encontrado.")

        # Botón para buscar al game master
        buscar_button = tk.Button(formulario_buscar, text="Buscar", command=buscar_game_master)
        buscar_button.pack()

    def mostrar_formulario_actualizar(self):
        # Crear una nueva ventana para el formulario de actualización
        formulario_actualizar = tk.Toplevel(self)
        formulario_actualizar.title("Actualizar Game Master")
        formulario_actualizar.geometry("300x200")

        # Campo de entrada para el ID de usuario
        id_usuario_label = tk.Label(formulario_actualizar, text="ID de Usuario:")
        id_usuario_label.pack()
        id_usuario_entry = tk.Entry(formulario_actualizar)
        id_usuario_entry.pack()

        # Función para buscar al game master en la base de datos y mostrar el formulario de actualización
        def buscar_game_master_actualizar():
            id_usuario = id_usuario_entry.get()

            # Validación del campo ID de usuario
            if not (4 <= len(id_usuario) <= 20):
                messagebox.showerror("Error", "ID de usuario inválido. Ingrese un string de largo entre 4 y 20 caracteres.")
                return

            # Buscar al game master en la base de datos
            game_master = game_masters_collection.find_one({"id_usuario": id_usuario})

            if game_master:
                # Cerrar la ventana de búsqueda
                formulario_actualizar.destroy()

                # Mostrar una nueva ventana con el formulario de actualización
                ventana_actualizar = tk.Toplevel(self)
                ventana_actualizar.title("Actualizar Game Master")
                ventana_actualizar.geometry("300x200")

                # Campos de entrada para los atributos actualizados
                nuevo_nombre_usuario_label = tk.Label(ventana_actualizar, text="Nuevo Nombre de Usuario:")
                nuevo_nombre_usuario_label.pack()
                nuevo_nombre_usuario_entry = tk.Entry(ventana_actualizar)
                nuevo_nombre_usuario_entry.pack()

                nueva_contrasena_label = tk.Label(ventana_actualizar, text="Nueva Contraseña:")
                nueva_contrasena_label.pack()
                nueva_contrasena_entry = tk.Entry(ventana_actualizar, show="*")
                nueva_contrasena_entry.pack()

                nuevo_correo_label = tk.Label(ventana_actualizar, text="Nuevo Correo:")
                nuevo_correo_label.pack()
                nuevo_correo_entry = tk.Entry(ventana_actualizar)
                nuevo_correo_entry.pack()

                # Función para actualizar los atributos en la base de datos
                def actualizar_game_master():
                    nuevo_nombre_usuario = nuevo_nombre_usuario_entry.get()
                    nueva_contrasena = nueva_contrasena_entry.get()
                    nuevo_correo = nuevo_correo_entry.get()

                    if not (4 <= len(nuevo_nombre_usuario) <= 20):
                        messagebox.showerror("Error", "Nombre de usuario inválido. Ingrese un string de largo entre 4 y 20 caracteres.")
                        return

                    if not (3 <= len(nueva_contrasena) <= 20):
                        messagebox.showerror("Error", "Contraseña inválida. Ingrese un string de largo entre 3 y 20 caracteres.")
                        return

                    if not (4 <= len(nuevo_correo) <= 50):
                        messagebox.showerror("Error", "Correo inválido. Ingrese un string de largo entre 4 y 50 caracteres.")
                        return

                    # Actualizar los datos en la base de datos
                    game_masters_collection.update_one({"id_usuario": id_usuario}, {"$set": {
                        "nombre_usuario": nuevo_nombre_usuario,
                        "contrasena": nueva_contrasena,
                        "correo": nuevo_correo,
                    }})

                    messagebox.showinfo("Éxito", "Game Master actualizado correctamente.")
                    ventana_actualizar.destroy()

                # Botón para actualizar el game master
                actualizar_button = tk.Button(ventana_actualizar, text="Actualizar", command=actualizar_game_master)
                actualizar_button.pack()
            else:
                messagebox.showerror("Error", "Game master no encontrado.")

        # Botón para buscar el jugador y mostrar el formulario de actualización
        buscar_button = tk.Button(formulario_actualizar, text="Buscar", command=buscar_game_master_actualizar)
        buscar_button.pack()

    def mostrar_formulario_eliminar(self):
        # Crear una nueva ventana para el formulario de eliminación
        formulario_eliminar = tk.Toplevel(self)
        formulario_eliminar.title("Eliminar Game Master")
        formulario_eliminar.geometry("300x150")

        # Campo de entrada para el ID de usuario
        id_usuario_label = tk.Label(formulario_eliminar, text="ID de Usuario:")
        id_usuario_label.pack()
        id_usuario_entry = tk.Entry(formulario_eliminar)
        id_usuario_entry.pack()

        # Función para buscar el game master en la base de datos y eliminarlo
        def buscar_game_master_eliminar():
            id_usuario = id_usuario_entry.get()

            # Validación del campo ID de usuario
            if not (4 <= len(id_usuario) <= 20):
                messagebox.showerror("Error", "ID de usuario inválido. Ingrese un string de largo entre 4 y 20 caracteres.")
                return

            # Buscar el game master en la base de datos
            game_master = game_masters_collection.find_one({"id_usuario": id_usuario})

            if game_master:
                # Eliminar el game master de la base de datos
                game_masters_collection.delete_one({"id_usuario": id_usuario})
                messagebox.showinfo("Éxito", "Game Master eliminado correctamente.")
                formulario_eliminar.destroy()
            else:
                messagebox.showerror("Error", "Game Master no encontrado.")

        # Botón para buscar el jugador y eliminarlo
        buscar_button = tk.Button(formulario_eliminar, text="Buscar", command=buscar_game_master_eliminar)
        buscar_button.pack()

# Interfaz para la colección Tabla Resumen
class TablaResumenInterface(tk.Toplevel):
    def __init__(self, parent):
        super().__init__(parent)
        self.title("Tabla Resumen")
        self.geometry("300x200")

        self.opciones_frame = tk.Frame(self)
        self.opciones_frame.pack()

        self.crear_button = tk.Button(self.opciones_frame, text="Crear", command=self.mostrar_formulario_crear)
        self.crear_button.pack(side=tk.LEFT)

        self.buscar_button = tk.Button(self.opciones_frame, text="Buscar", command=self.mostrar_formulario_buscar)
        self.buscar_button.pack(side=tk.LEFT)

        self.actualizar_button = tk.Button(self.opciones_frame, text="Actualizar", command=self.mostrar_formulario_actualizar)
        self.actualizar_button.pack(side=tk.LEFT)

        self.eliminar_button = tk.Button(self.opciones_frame, text="Eliminar", command=self.mostrar_formulario_eliminar)
        self.eliminar_button.pack(side=tk.LEFT)

    def mostrar_formulario_crear(self):
        # Crear una nueva ventana para el formulario de creación
        formulario_crear = tk.Toplevel(self)
        formulario_crear.title("Crear Tabla Resumen")
        formulario_crear.geometry("300x250")

        # Campos del formulario
        tabla_resumen_label = tk.Label(formulario_crear, text="Tabla resumen (formato JSON):")
        tabla_resumen_label.pack()
        tabla_resumen_entry = tk.Entry(formulario_crear)
        tabla_resumen_entry.pack()

        # Función para guardar los datos de la tabla resumen en la base de datos
        def guardar_tabla_resumen():
            tabla_resumen = tabla_resumen_entry.get()

            # Validación del campo
            if not (4 <= len(tabla_resumen) <= 300):
                messagebox.showerror("Error", "Nombre de usuario inválido. Ingrese un string de largo entre 4 y 300 caracteres.")
                return
            
            # Crear la tabla resumen en la base de datos
            tabla_resumen = {
                "tabla_resumen": tabla_resumen
            }

            tabla_resumen_collection.insert_one(tabla_resumen)
            messagebox.showinfo("Éxito", "Tabla resumen guardada correctamente.")

            # Cerrar la ventana del formulario después de guardar los datos
            formulario_crear.destroy()

        # Botón para guardar los datos
        guardar_button = tk.Button(formulario_crear, text="Guardar", command=guardar_tabla_resumen)
        guardar_button.pack()

    def mostrar_formulario_buscar(self):

        # Crear una nueva ventana para el formulario de búsqueda
        formulario_buscar = tk.Toplevel(self)
        formulario_buscar.title("Buscar Tabla Resumen")
        formulario_buscar.geometry("300x150")

        # Función para buscar la tabla resumen en la base de datos
        def buscar_tabla_resumen():
            # Buscar el primer documento de la colección tabla_resumen
            primer_documento = tabla_resumen_collection.find_one()

            # Verificar si se encontró un documento
            if primer_documento:
                # Obtener el valor del campo "tabla_resumen"
                tabla_resumen = primer_documento.get("tabla_resumen")

                if tabla_resumen:
                    # Mostrar el valor de "tabla_resumen" en una nueva ventana
                    ventana_tabla_resumen = tk.Toplevel(formulario_buscar)
                    ventana_tabla_resumen.title("Tabla Resumen Encontrada")
                    ventana_tabla_resumen.geometry("300x200")

                    # Mostrar el valor de "tabla_resumen"
                    tabla_resumen_label = tk.Label(ventana_tabla_resumen, text=f"Tabla resumen: {tabla_resumen}")
                    tabla_resumen_label.pack()
                else:
                    # Mostrar un mensaje si el campo "tabla_resumen" está vacío
                    messagebox.showinfo("Tabla Resumen", "El campo 'tabla_resumen' está vacío.")
            else:
                # Mostrar un mensaje si no se encuentra ningún documento
                messagebox.showinfo("Tabla Resumen", "No se encontraron documentos en la colección 'tabla_resumen'.")

        # Botón para buscar la tabla resumen
        buscar_button = tk.Button(formulario_buscar, text="Buscar", command=buscar_tabla_resumen)
        buscar_button.pack()

    def mostrar_formulario_actualizar(self):
        # Crear una nueva ventana para el formulario de actualización
        formulario_actualizar = tk.Toplevel(self)
        formulario_actualizar.title("Actualizar Tabla Resumen")
        formulario_actualizar.geometry("300x200")


        # Función para buscar la tabla resumen en la base de datos y mostrar el formulario de actualización
        def buscar_tabla_resumen_actualizar():

            # Buscar la tabla resumen en la base de datos
            tabla_resumen = tabla_resumen_collection.find({})

            if tabla_resumen:
                # Cerrar la ventana de búsqueda
                formulario_actualizar.destroy()

                # Mostrar una nueva ventana con el formulario de actualización
                ventana_actualizar = tk.Toplevel(self)
                ventana_actualizar.title("Actualizar Tabla Resumen")
                ventana_actualizar.geometry("300x200")

                # Campos de entrada para los atributos actualizados
                nuevo_tabla_resumen_label = tk.Label(ventana_actualizar, text="Nueva Tabla Resumen:")
                nuevo_tabla_resumen_label.pack()
                nuevo_tabla_resumen_entry = tk.Entry(ventana_actualizar)
                nuevo_tabla_resumen_entry.pack()

                # Función para actualizar los atributos en la base de datos
                def actualizar_tabla_resumen():
                    nuevo_tabla_resumen = nuevo_tabla_resumen_entry.get()

                    if not (4 <= len(nuevo_tabla_resumen) <= 300):
                        messagebox.showerror("Error", "Tabla resumen inválida. Ingrese un string JSON de largo entre 4 y 300 caracteres.")
                        return

                    # Actualizar los datos en la base de datos

                    tabla_resumen_collection.delete_many({})

                    tabla_resumen_collection.insert_one({
                        "tabla_resumen": nuevo_tabla_resumen
                    })

                    messagebox.showinfo("Éxito", "Tabla resumen actualizada correctamente.")
                    ventana_actualizar.destroy()

                # Botón para actualizar la tabla resumen
                actualizar_button = tk.Button(ventana_actualizar, text="Actualizar", command=actualizar_tabla_resumen)
                actualizar_button.pack()

            else:
                # Cerrar la ventana de búsqueda
                formulario_actualizar.destroy()

                # Mostrar una nueva ventana con el formulario de actualización
                ventana_actualizar = tk.Toplevel(self)
                ventana_actualizar.title("Actualizar Tabla Resumen")
                ventana_actualizar.geometry("300x200")

                # Campos de entrada para los atributos actualizados
                nuevo_tabla_resumen_label = tk.Label(ventana_actualizar, text="Nueva Tabla Resumen:")
                nuevo_tabla_resumen_label.pack()
                nuevo_tabla_resumen_entry = tk.Entry(ventana_actualizar)
                nuevo_tabla_resumen_entry.pack()

                # Función para actualizar los atributos en la base de datos
                def actualizar_tabla_resumen():
                    nuevo_tabla_resumen = nuevo_tabla_resumen_entry.get()

                    if not (4 <= len(nuevo_tabla_resumen) <= 300):
                        messagebox.showerror("Error", "Tabla resumen inválida. Ingrese un string JSON de largo entre 4 y 300 caracteres.")
                        return

                    # Actualizar los datos en la base de datos

                    tabla_resumen_collection.insert_one({
                        "tabla_resumen": tabla_resumen
                    })

                    messagebox.showinfo("Éxito", "Tabla resumen actualizada correctamente.")
                    ventana_actualizar.destroy()

                # Botón para actualizar la tabla resumen
                actualizar_button = tk.Button(ventana_actualizar, text="Actualizar", command=actualizar_tabla_resumen)
                actualizar_button.pack()

        # Botón para buscar el jugador y mostrar el formulario de actualización
        buscar_button = tk.Button(formulario_actualizar, text="Buscar", command=buscar_tabla_resumen_actualizar)
        buscar_button.pack()

    def mostrar_formulario_eliminar(self):
        # Crear una nueva ventana para el formulario de eliminación
        formulario_eliminar = tk.Toplevel(self)
        formulario_eliminar.title("Eliminar Tabla Resumen")
        formulario_eliminar.geometry("300x150")

        # Función para buscar la tabla resumen en la base de datos y eliminarla
        def buscar_tabla_resumen_eliminar():
            
            tabla_resumen_collection.delete_many({})
            messagebox.showinfo("Éxito", "Tabla resumen eliminada correctamente.")
            formulario_eliminar.destroy()

        # Botón para buscar el jugador y eliminarlo
        buscar_button = tk.Button(formulario_eliminar, text="Presione para eliminar", command=buscar_tabla_resumen_eliminar)
        buscar_button.pack()

# Interfaz para la colección listado_habilidades
class ListadoHabilidadesInterface(tk.Toplevel):
    def __init__(self, parent):
        super().__init__(parent)
        self.title("Listado de habilidades")
        self.geometry("300x200")

        self.opciones_frame = tk.Frame(self)
        self.opciones_frame.pack()

        self.crear_button = tk.Button(self.opciones_frame, text="Crear", command=self.mostrar_formulario_crear)
        self.crear_button.pack(side=tk.LEFT)

        self.buscar_button = tk.Button(self.opciones_frame, text="Buscar", command=self.mostrar_formulario_buscar)
        self.buscar_button.pack(side=tk.LEFT)

        self.actualizar_button = tk.Button(self.opciones_frame, text="Actualizar", command=self.mostrar_formulario_actualizar)
        self.actualizar_button.pack(side=tk.LEFT)

        self.eliminar_button = tk.Button(self.opciones_frame, text="Eliminar", command=self.mostrar_formulario_eliminar)
        self.eliminar_button.pack(side=tk.LEFT)

    def mostrar_formulario_crear(self):
        # Crear una nueva ventana para el formulario de creación
        formulario_crear = tk.Toplevel(self)
        formulario_crear.title("Crear Listado de Habilidades")
        formulario_crear.geometry("300x250")

        # Campos del formulario
        listado_habilidades_label = tk.Label(formulario_crear, text="Listado de habilidades (formato JSON):")
        listado_habilidades_label.pack()
        listado_habilidades_entry = tk.Entry(formulario_crear)
        listado_habilidades_entry.pack()

        # Función para guardar los datos de la tabla resumen en la base de datos
        def guardar_listado_habilidades():
            listado_habilidades = listado_habilidades_entry.get()

            # Validación del campo
            if not (4 <= len(listado_habilidades) <= 300):
                messagebox.showerror("Error", "Listado de habilidades inválido. Ingrese un string JSON de largo entre 4 y 300 caracteres.")
                return
            
            # Crear la tabla resumen en la base de datos
            listado_habilidades = {
                "listado_habilidades": listado_habilidades
            }

            listado_habilidades_collection.insert_one(listado_habilidades)
            messagebox.showinfo("Éxito", "Listado de habilidades guardado correctamente.")

            # Cerrar la ventana del formulario después de guardar los datos
            formulario_crear.destroy()

        # Botón para guardar los datos
        guardar_button = tk.Button(formulario_crear, text="Guardar", command=guardar_listado_habilidades)
        guardar_button.pack()

    def mostrar_formulario_buscar(self):

        # Crear una nueva ventana para el formulario de búsqueda
        formulario_buscar = tk.Toplevel(self)
        formulario_buscar.title("Buscar Listado de Habilidades")
        formulario_buscar.geometry("300x150")

        # Función para buscar el listado de habilidades en la base de datos
        def buscar_listado_habilidades():
            # Buscar el primer documento de la colección listado_habilidades
            primer_documento = listado_habilidades_collection.find_one()

            # Verificar si se encontró un documento
            if primer_documento:
                # Obtener el valor del campo "listado_habilidades"
                listado_habilidades = primer_documento.get("listado_habilidades")

                if listado_habilidades:
                    # Mostrar el valor de "listado_habilidades" en una nueva ventana
                    ventana_listado_habilidades = tk.Toplevel(formulario_buscar)
                    ventana_listado_habilidades.title("Listado de Habilidades Encontrado")
                    ventana_listado_habilidades.geometry("300x200")

                    # Mostrar el valor de "listado_habilidades"
                    listado_habilidades_label = tk.Label(ventana_listado_habilidades, text=f"Listado de habilidades: {listado_habilidades}")
                    listado_habilidades_label.pack()
                else:
                    # Mostrar un mensaje si el campo "listado_habilidades" está vacío
                    messagebox.showinfo("Listado de habilidades", "El campo 'listado_habilidades' está vacío.")
            else:
                # Mostrar un mensaje si no se encuentra ningún documento
                messagebox.showinfo("Listado de habilidades", "No se encontraron documentos en la colección 'listado_habilidades'.")

        # Botón para buscar la tabla resumen
        buscar_button = tk.Button(formulario_buscar, text="Buscar", command=buscar_listado_habilidades)
        buscar_button.pack()

    def mostrar_formulario_actualizar(self):
        # Crear una nueva ventana para el formulario de actualización
        formulario_actualizar = tk.Toplevel(self)
        formulario_actualizar.title("Actualizar Listado de Habilidades")
        formulario_actualizar.geometry("300x200")


        # Función para buscar el listado de habilidades en la base de datos y mostrar el formulario de actualización
        def buscar_listado_habilidades_actualizar():

            # Buscar el listado de habilidades en la base de datos
            listado_habilidades = listado_habilidades_collection.find({})

            if listado_habilidades:
                # Cerrar la ventana de búsqueda
                formulario_actualizar.destroy()

                # Mostrar una nueva ventana con el formulario de actualización
                ventana_actualizar = tk.Toplevel(self)
                ventana_actualizar.title("Actualizar Listado de Habilidades")
                ventana_actualizar.geometry("300x200")

                # Campos de entrada para los atributos actualizados
                nuevo_listado_habilidades_label = tk.Label(ventana_actualizar, text="Nuevo Listado de Habilidades:")
                nuevo_listado_habilidades_label.pack()
                nuevo_listado_habilidades_entry = tk.Entry(ventana_actualizar)
                nuevo_listado_habilidades_entry.pack()

                # Función para actualizar los atributos en la base de datos
                def actualizar_listado_habilidades():
                    nuevo_listado_habilidades = nuevo_listado_habilidades_entry.get()

                    if not (4 <= len(nuevo_listado_habilidades) <= 300):
                        messagebox.showerror("Error", "Listado de habilidades inválido. Ingrese un string JSON de largo entre 4 y 300 caracteres.")
                        return

                    # Actualizar los datos en la base de datos

                    listado_habilidades_collection.delete_many({})

                    listado_habilidades_collection.insert_one({
                        "listado_habilidades": nuevo_listado_habilidades
                    })

                    messagebox.showinfo("Éxito", "Listado de habilidades actualizada correctamente.")
                    ventana_actualizar.destroy()

                # Botón para actualizar el listado de habilidades
                actualizar_button = tk.Button(ventana_actualizar, text="Actualizar", command=actualizar_listado_habilidades)
                actualizar_button.pack()

            else:
                # Cerrar la ventana de búsqueda
                formulario_actualizar.destroy()

                # Mostrar una nueva ventana con el formulario de actualización
                ventana_actualizar = tk.Toplevel(self)
                ventana_actualizar.title("Actualizar Listado de Habilidades")
                ventana_actualizar.geometry("300x200")

                # Campos de entrada para los atributos actualizados
                nuevo_listado_habilidades_label = tk.Label(ventana_actualizar, text="Nuevo Listado de Habilidades:")
                nuevo_listado_habilidades_label.pack()
                nuevo_listado_habilidades_entry = tk.Entry(ventana_actualizar)
                nuevo_listado_habilidades_entry.pack()

                # Función para actualizar los atributos en la base de datos
                def actualizar_listado_habilidades():
                    nuevo_listado_habilidades = nuevo_listado_habilidades_entry.get()

                    if not (4 <= len(nuevo_listado_habilidades) <= 300):
                        messagebox.showerror("Error", "Listado de habilidades inválido. Ingrese un string JSON de largo entre 4 y 300 caracteres.")
                        return

                    # Actualizar los datos en la base de datos

                    listado_habilidades_collection.insert_one({
                        "listado_habilidades": listado_habilidades
                    })

                    messagebox.showinfo("Éxito", "Listado de habilidades actualizado correctamente.")
                    ventana_actualizar.destroy()

                # Botón para actualizar el listado de habilidades
                actualizar_button = tk.Button(ventana_actualizar, text="Actualizar", command=actualizar_listado_habilidades)
                actualizar_button.pack()

        # Botón para buscar el listado de habilidades y mostrar el formulario de actualización
        buscar_button = tk.Button(formulario_actualizar, text="Buscar", command=buscar_listado_habilidades_actualizar)
        buscar_button.pack()

    def mostrar_formulario_eliminar(self):
        # Crear una nueva ventana para el formulario de eliminación
        formulario_eliminar = tk.Toplevel(self)
        formulario_eliminar.title("Eliminar Listado de Habilidades")
        formulario_eliminar.geometry("300x150")

        # Función para buscar el listado de habilidades en la base de datos y eliminarlo
        def buscar_listado_habilidades_eliminar():            
            listado_habilidades_collection.delete_many({})
            messagebox.showinfo("Éxito", "Listado de habilidades eliminado correctamente.")
            formulario_eliminar.destroy()

        # Botón para buscar el listado de habilidades y eliminarlo
        buscar_button = tk.Button(formulario_eliminar, text="Presione para eliminar", command=buscar_listado_habilidades_eliminar)
        buscar_button.pack()

class PersonajesInterface(tk.Toplevel):
    def __init__(self, parent):
        super().__init__(parent)
        self.title("Personajes")
        self.geometry("300x200")

        self.opciones_frame = tk.Frame(self)
        self.opciones_frame.pack()

        self.crear_button = tk.Button(self.opciones_frame, text="Crear", command=self.mostrar_formulario_crear)
        self.crear_button.pack(side=tk.LEFT)

        self.buscar_button = tk.Button(self.opciones_frame, text="Buscar", command=self.mostrar_formulario_buscar)
        self.buscar_button.pack(side=tk.LEFT)

        self.actualizar_button = tk.Button(self.opciones_frame, text="Actualizar", command=self.mostrar_formulario_actualizar)
        self.actualizar_button.pack(side=tk.LEFT)

        self.eliminar_button = tk.Button(self.opciones_frame, text="Eliminar", command=self.mostrar_formulario_eliminar)
        self.eliminar_button.pack(side=tk.LEFT)

    def mostrar_formulario_crear(self):
        # Crear una nueva ventana para el formulario de creación
        formulario_crear = tk.Toplevel(self)
        formulario_crear.title("Crear Personaje")
        formulario_crear.geometry("300x450")

        # Campos del formulario
        nivel_personaje_label = tk.Label(formulario_crear, text="Nivel:")
        nivel_personaje_label.pack()
        nivel_personaje_entry = tk.Entry(formulario_crear)
        nivel_personaje_entry.pack()

        nombre_personaje_label = tk.Label(formulario_crear, text="Nombre:")
        nombre_personaje_label.pack()
        nombre_personaje_entry = tk.Entry(formulario_crear)
        nombre_personaje_entry.pack()

        edad_personaje_label = tk.Label(formulario_crear, text="Edad:")
        edad_personaje_label.pack()
        edad_personaje_entry = tk.Entry(formulario_crear)
        edad_personaje_entry.pack()

        sexo_personaje_label = tk.Label(formulario_crear, text="Sexo (H o M):")
        sexo_personaje_label.pack()
        sexo_personaje_entry = tk.Entry(formulario_crear)
        sexo_personaje_entry.pack()

        raza_personaje_label = tk.Label(formulario_crear, text="Raza:")
        raza_personaje_label.pack()
        raza_personaje_entry = tk.Entry(formulario_crear)
        raza_personaje_entry.pack()

        habilidades_personaje_label = tk.Label(formulario_crear, text="Habilidades:")
        habilidades_personaje_label.pack()
        habilidades_personaje_entry = tk.Entry(formulario_crear)
        habilidades_personaje_entry.pack()

        poder_personaje_label = tk.Label(formulario_crear, text="Poder:")
        poder_personaje_label.pack()
        poder_personaje_entry = tk.Entry(formulario_crear)
        poder_personaje_entry.pack()

        equipamiento_personaje_label = tk.Label(formulario_crear, text="Equipamiento:")
        equipamiento_personaje_label.pack()
        equipamiento_personaje_entry = tk.Entry(formulario_crear)
        equipamiento_personaje_entry.pack()

        estado_personaje_label = tk.Label(formulario_crear, text="Estado:")
        estado_personaje_label.pack()
        estado_personaje_entry = tk.Entry(formulario_crear)
        estado_personaje_entry.pack()

        estadisticas_personaje_label = tk.Label(formulario_crear, text="Estadísticas:")
        estadisticas_personaje_label.pack()
        estadisticas_personaje_entry = tk.Entry(formulario_crear)
        estadisticas_personaje_entry.pack()

        # Función para guardar los datos del personaje en la base de datos
        def guardar_personaje():
            nivel_personaje = nivel_personaje_entry.get()
            nombre_personaje = nombre_personaje_entry.get()
            edad_personaje = edad_personaje_entry.get()
            sexo_personaje = sexo_personaje_entry.get()
            raza_personaje = raza_personaje_entry.get()
            habilidades_personaje = habilidades_personaje_entry.get()
            poder_personaje = poder_personaje_entry.get()
            equipamiento_personaje = equipamiento_personaje_entry.get()
            estado_personaje = estado_personaje_entry.get()
            estadisticas_personaje = estadisticas_personaje_entry.get()

            # Validación del campo nivel
            try:
                nivel = int(nivel_personaje)
                if not (1 <= nivel <= 99):
                    raise ValueError
            except ValueError:
                messagebox.showerror("Error", "Nivel inválido. Ingrese un entero positivo entre 1 y 99.")
                return
            
            # Validación del campo nombre
            if not (3 <= len(nombre_personaje) <= 15):
                messagebox.showerror("Error", "Nombre inválido. Ingrese un string de largo entre 3 y 15 caracteres.")
                return
            
            # Validación del campo edad
            try:
                if not (5 <= int(edad_personaje) <= 90):
                    raise ValueError
            except ValueError:
                messagebox.showerror("Error", "Edad inválida. Ingrese un entero positivo entre 5 y 90.")
                return
            
            # Validación del campo sexo
            if sexo_personaje not in ['H', 'M']:
                messagebox.showerror("Error", "Sexo inválido. Ingrese 'H' o 'M'.")
                return
            
            # Validación del campo raza
            if not (4 <= len(raza_personaje) <= 10):
                messagebox.showerror("Error", "Raza inválida. Ingrese un string de largo entre 4 y 10 caracteres.")
                return
            
            # Validación del campo habilidades
            if not (5 <= len(habilidades_personaje) <= 30):
                messagebox.showerror("Error", "Habilidades inválidas. Ingrese un string de largo entre 5 y 30 caracteres.")
                return
            
            # Validación del campo poder
            if not (3 <= len(poder_personaje) <= 15):
                messagebox.showerror("Error", "Poder inválido. Ingrese un string de largo entre 3 y 15 caracteres.")
                return
            
            # Validación del campo equipamiento
            if not (5 <= len(equipamiento_personaje) <= 30):
                messagebox.showerror("Error", "Equipamiento inválido. Ingrese un string de largo entre 5 y 30 caracteres.")
                return
            
            # Validación del campo estado
            valid_estados = ["vivo", "muerto", "congelado", "envenenado", "quemado"]
            if estado_personaje.lower() not in valid_estados:
                messagebox.showerror("Error", "Estado inválido. Ingrese uno de los siguientes valores: Vivo, Muerto, Congelado, Envenenado, Quemado.")
                return
            
            # Validación del campo estadísticas
            if not (5 <= len(estadisticas_personaje) <= 50):
                messagebox.showerror("Error", "Estadísticas inválidas. Ingrese un string de largo entre 5 y 50 caracteres.")
                return

            # Crear al personaje en la base de datos
            personaje = {
                "nivel": nivel_personaje,
                "nombre": nombre_personaje,
                "edad": edad_personaje,
                "sexo": sexo_personaje,
                "raza": raza_personaje,
                "habilidades": habilidades_personaje,
                "poder": poder_personaje,
                "equipamiento": equipamiento_personaje,
                "estado": estado_personaje,
                "estadisticas": estadisticas_personaje
            }

            personajes_collection.insert_one(personaje)
            messagebox.showinfo("Éxito", "Personaje guardado correctamente.")

            # Cerrar la ventana del formulario después de guardar los datos
            formulario_crear.destroy()

        # Botón para guardar los datos
        guardar_button = tk.Button(formulario_crear, text="Guardar", command=guardar_personaje)
        guardar_button.pack()

    def mostrar_formulario_buscar(self):

        # Crear una nueva ventana para el formulario de búsqueda
        formulario_buscar = tk.Toplevel(self)
        formulario_buscar.title("Buscar Personaje")
        formulario_buscar.geometry("300x150")

        # Campo de entrada para el nombre de personaje
        nombre_personaje_label = tk.Label(formulario_buscar, text="Nombre del personaje:")
        nombre_personaje_label.pack()
        nombre_personaje_entry = tk.Entry(formulario_buscar)
        nombre_personaje_entry.pack()

        # Función para buscar al personaje en la base de datos
        def buscar_personaje():
            nombre_personaje = nombre_personaje_entry.get()

            # Validación del campo nombre de personaje
            if not (4 <= len(nombre_personaje) <= 20):
                messagebox.showerror("Error", "Nombre de personaje inválido. Ingrese un string de largo entre 4 y 20 caracteres.")
                return

            # Buscar al personaje en la base de datos
            personaje = personajes_collection.find_one({"nombre": nombre_personaje})

            if personaje:
                # Mostrar los atributos del personaje en una nueva ventana
                ventana_personaje = tk.Toplevel(formulario_buscar)
                ventana_personaje.title("Personaje Encontrado")
                ventana_personaje.geometry("300x450")

                # Mostrar los atributos del personaje
                nivel_personaje_label = tk.Label(ventana_personaje, text=f"Nivel del Personaje: {personaje['nivel']}")
                nivel_personaje_label.pack()

                nombre_personaje_label = tk.Label(ventana_personaje, text=f"Nombre del Personaje: {personaje['nombre']}")
                nombre_personaje_label.pack()

                edad_personaje_label = tk.Label(ventana_personaje, text=f"Edad del Personaje: {personaje['edad']}")
                edad_personaje_label.pack()

                sexo_personaje_label = tk.Label(ventana_personaje, text=f"Sexo del Personaje: {personaje['sexo']}")
                sexo_personaje_label.pack()

                raza_personaje_label = tk.Label(ventana_personaje, text=f"Raza del Personaje: {personaje['raza']}")
                raza_personaje_label.pack()

                habilidades_personaje_label = tk.Label(ventana_personaje, text=f"Habilidades del Personaje: {personaje['habilidades']}")
                habilidades_personaje_label.pack()

                poder_personaje_label = tk.Label(ventana_personaje, text=f"Poderes del Personaje: {personaje['poder']}")
                poder_personaje_label.pack()

                equipamiento_personaje_label = tk.Label(ventana_personaje, text=f"Equipamiento del Personaje: {personaje['equipamiento']}")
                equipamiento_personaje_label.pack()

                estado_personaje_label = tk.Label(ventana_personaje, text=f"Estado del Personaje: {personaje['estado']}")
                estado_personaje_label.pack()

                estadisticas_personaje_label = tk.Label(ventana_personaje, text=f"Estadisticas del Personaje: {personaje['estadisticas']}")
                estadisticas_personaje_label.pack()

            else:
                messagebox.showerror("Error", "Personaje no encontrado.")

        # Botón para buscar al game master
        buscar_button = tk.Button(formulario_buscar, text="Buscar", command=buscar_personaje)
        buscar_button.pack()

    def mostrar_formulario_actualizar(self):
        # Crear una nueva ventana para el formulario de actualización
        formulario_actualizar = tk.Toplevel(self)
        formulario_actualizar.title("Actualizar Personaje")
        formulario_actualizar.geometry("300x200")

        # Campo de entrada para el nombre del personaje
        nombre_personaje_label = tk.Label(formulario_actualizar, text="Nombre del Personaje:")
        nombre_personaje_label.pack()
        nombre_personaje_entry = tk.Entry(formulario_actualizar)
        nombre_personaje_entry.pack()

        # Función para buscar al personaje en la base de datos y mostrar el formulario de actualización
        def buscar_personaje_actualizar():
            nombre_personaje = nombre_personaje_entry.get()

            # Validación del campo ID de usuario
            if not (4 <= len(nombre_personaje) <= 20):
                messagebox.showerror("Error", "Nombre de personaje inválido. Ingrese un string de largo entre 4 y 20 caracteres.")
                return

            # Buscar al personaje en la base de datos
            personaje = personajes_collection.find_one({"nombre": nombre_personaje})

            if personaje:
                # Cerrar la ventana de búsqueda
                formulario_actualizar.destroy()

                # Mostrar una nueva ventana con el formulario de actualización
                ventana_actualizar = tk.Toplevel(self)
                ventana_actualizar.title("Actualizar Personaje")
                ventana_actualizar.geometry("300x450")

                # Campos de entrada para los atributos actualizados
                nuevo_nivel_personaje_label = tk.Label(ventana_actualizar, text="Nivel:")
                nuevo_nivel_personaje_label.pack()
                nuevo_nivel_personaje_entry = tk.Entry(ventana_actualizar)
                nuevo_nivel_personaje_entry.pack()

                nuevo_nombre_personaje_label = tk.Label(ventana_actualizar, text="Nombre:")
                nuevo_nombre_personaje_label.pack()
                nuevo_nombre_personaje_entry = tk.Entry(ventana_actualizar)
                nuevo_nombre_personaje_entry.pack()

                nuevo_edad_personaje_label = tk.Label(ventana_actualizar, text="Edad:")
                nuevo_edad_personaje_label.pack()
                nuevo_edad_personaje_entry = tk.Entry(ventana_actualizar)
                nuevo_edad_personaje_entry.pack()

                nuevo_sexo_personaje_label = tk.Label(ventana_actualizar, text="Sexo (H o M):")
                nuevo_sexo_personaje_label.pack()
                nuevo_sexo_personaje_entry = tk.Entry(ventana_actualizar)
                nuevo_sexo_personaje_entry.pack()

                nuevo_raza_personaje_label = tk.Label(ventana_actualizar, text="Raza:")
                nuevo_raza_personaje_label.pack()
                nuevo_raza_personaje_entry = tk.Entry(ventana_actualizar)
                nuevo_raza_personaje_entry.pack()

                nuevo_habilidades_personaje_label = tk.Label(ventana_actualizar, text="Habilidades:")
                nuevo_habilidades_personaje_label.pack()
                nuevo_habilidades_personaje_entry = tk.Entry(ventana_actualizar)
                nuevo_habilidades_personaje_entry.pack()

                nuevo_poder_personaje_label = tk.Label(ventana_actualizar, text="Poder:")
                nuevo_poder_personaje_label.pack()
                nuevo_poder_personaje_entry = tk.Entry(ventana_actualizar)
                nuevo_poder_personaje_entry.pack()

                nuevo_equipamiento_personaje_label = tk.Label(ventana_actualizar, text="Equipamiento:")
                nuevo_equipamiento_personaje_label.pack()
                nuevo_equipamiento_personaje_entry = tk.Entry(ventana_actualizar)
                nuevo_equipamiento_personaje_entry.pack()

                nuevo_estado_personaje_label = tk.Label(ventana_actualizar, text="Estado:")
                nuevo_estado_personaje_label.pack()
                nuevo_estado_personaje_entry = tk.Entry(ventana_actualizar)
                nuevo_estado_personaje_entry.pack()

                nuevo_estadisticas_personaje_label = tk.Label(ventana_actualizar, text="Estadísticas:")
                nuevo_estadisticas_personaje_label.pack()
                nuevo_estadisticas_personaje_entry = tk.Entry(ventana_actualizar)
                nuevo_estadisticas_personaje_entry.pack()

                # Función para actualizar los atributos en la base de datos
                def actualizar_personaje():
                    nuevo_nivel_personaje = nuevo_nivel_personaje_entry.get()
                    nuevo_nombre_personaje = nuevo_nombre_personaje_entry.get()
                    nuevo_edad_personaje = nuevo_edad_personaje_entry.get()
                    nuevo_sexo_personaje = nuevo_sexo_personaje_entry.get()
                    nuevo_raza_personaje = nuevo_raza_personaje_entry.get()
                    nuevo_habilidades_personaje= nuevo_habilidades_personaje_entry.get()
                    nuevo_poder_personaje= nuevo_poder_personaje_entry.get()
                    nuevo_equipamiento_personaje= nuevo_equipamiento_personaje_entry.get()
                    nuevo_estado_personaje= nuevo_estado_personaje_entry.get()
                    nuevo_estadisticas_personaje = nuevo_estadisticas_personaje_entry.get()
                    
                    # Validación del campo nivel
                    try:
                        nivel = int(nuevo_nivel_personaje)
                        if not (1 <= nivel <= 99):
                            raise ValueError
                    except ValueError:
                        messagebox.showerror("Error", "Nivel inválido. Ingrese un entero positivo entre 1 y 99.")
                        return
                    
                    # Validación del campo nombre
                    if not (3 <= len(nuevo_nombre_personaje) <= 15):
                        messagebox.showerror("Error", "Nombre inválido. Ingrese un string de largo entre 3 y 15 caracteres.")
                        return
                    
                    # Validación del campo edad
                    try:
                        if not (5 <= int(nuevo_edad_personaje) <= 90):
                            raise ValueError
                    except ValueError:
                        messagebox.showerror("Error", "Edad inválida. Ingrese un entero positivo entre 5 y 90.")
                        return
                    
                    # Validación del campo sexo
                    if nuevo_sexo_personaje not in ['H', 'M']:
                        messagebox.showerror("Error", "Sexo inválido. Ingrese 'H' o 'M'.")
                        return
                    
                    # Validación del campo raza
                    if not (4 <= len(nuevo_raza_personaje) <= 10):
                        messagebox.showerror("Error", "Raza inválida. Ingrese un string de largo entre 4 y 10 caracteres.")
                        return
                    
                    # Validación del campo habilidades
                    if not (5 <= len(nuevo_habilidades_personaje) <= 30):
                        messagebox.showerror("Error", "Habilidades inválidas. Ingrese un string de largo entre 5 y 30 caracteres.")
                        return
                    
                    # Validación del campo poder
                    if not (3 <= len(nuevo_poder_personaje) <= 15):
                        messagebox.showerror("Error", "Poder inválido. Ingrese un string de largo entre 3 y 15 caracteres.")
                        return
                    
                    # Validación del campo equipamiento
                    if not (5 <= len(nuevo_equipamiento_personaje) <= 30):
                        messagebox.showerror("Error", "Equipamiento inválido. Ingrese un string de largo entre 5 y 30 caracteres.")
                        return
                    
                    # Validación del campo estado
                    valid_estados = ["vivo", "muerto", "congelado", "envenenado", "quemado"]
                    if nuevo_estado_personaje.lower() not in valid_estados:
                        messagebox.showerror("Error", "Estado inválido. Ingrese uno de los siguientes valores: Vivo, Muerto, Congelado, Envenenado, Quemado.")
                        return
                    
                    # Validación del campo estadísticas
                    if not (5 <= len(nuevo_estadisticas_personaje) <= 50):
                        messagebox.showerror("Error", "Estadísticas inválidas. Ingrese un string de largo entre 5 y 50 caracteres.")
                        return

                    # Actualizar los datos en la base de datos
                    personajes_collection.update_one({"nombre": nombre_personaje}, {"$set": {
                        "nivel": nuevo_nivel_personaje,
                        "nombre": nuevo_nombre_personaje,
                        "edad": nuevo_edad_personaje,
                        "sexo": nuevo_sexo_personaje,
                        "raza": nuevo_raza_personaje,
                        "habilidades": nuevo_habilidades_personaje,
                        "poder": nuevo_poder_personaje,
                        "equipamiento": nuevo_equipamiento_personaje,
                        "estado": nuevo_estado_personaje,
                        "estadisticas": nuevo_estadisticas_personaje
                    }})

                    messagebox.showinfo("Éxito", "Personaje actualizado correctamente.")
                    ventana_actualizar.destroy()

                # Botón para actualizar el personaje
                actualizar_button = tk.Button(ventana_actualizar, text="Actualizar", command=actualizar_personaje)
                actualizar_button.pack()
            else:
                messagebox.showerror("Error", "Personaje no encontrado.")

        # Botón para buscar el personaje y mostrar el formulario de actualización
        buscar_button = tk.Button(formulario_actualizar, text="Buscar", command=buscar_personaje_actualizar)
        buscar_button.pack()

    def mostrar_formulario_eliminar(self):
        # Crear una nueva ventana para el formulario de eliminación
        formulario_eliminar = tk.Toplevel(self)
        formulario_eliminar.title("Eliminar Personaje")
        formulario_eliminar.geometry("300x150")

        # Campo de entrada para el nombre de personaje
        nombre_label = tk.Label(formulario_eliminar, text="Nombre de personaje:")
        nombre_label.pack()
        nombre_entry = tk.Entry(formulario_eliminar)
        nombre_entry.pack()

        # Función para buscar el personaje en la base de datos y eliminarlo
        def buscar_personaje_eliminar():
            nombre = nombre_entry.get()

            # Validación del campo nombre del personaje
            if not (4 <= len(nombre) <= 20):
                messagebox.showerror("Error", "Nombre de personaje inválido. Ingrese un string de largo entre 4 y 20 caracteres.")
                return

            # Buscar el personaje en la base de datos
            personaje = personajes_collection.find_one({"nombre": nombre})

            if personaje:
                # Eliminar el personaje de la base de datos
                personajes_collection.delete_one({"nombre": nombre})
                messagebox.showinfo("Éxito", "Personaje eliminado correctamente.")
                formulario_eliminar.destroy()
            else:
                messagebox.showerror("Error", "Personaje no encontrado.")

        # Botón para buscar el jugador y eliminarlo
        buscar_button = tk.Button(formulario_eliminar, text="Buscar", command=buscar_personaje_eliminar)
        buscar_button.pack()

class MainInterface(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Sistema de Juego RPG")
        self.geometry("400x300")

        self.jugadores_button = tk.Button(self, text="Jugadores", command=self.mostrar_jugadores_interface)
        self.jugadores_button.pack(pady=10)

        self.game_masters_button = tk.Button(self, text="Game Masters", command=self.mostrar_game_masters_interface)
        self.game_masters_button.pack(pady=10)

        self.tabla_resumen_button = tk.Button(self, text="Tabla Resumen", command=self.mostrar_tabla_resumen_interface)
        self.tabla_resumen_button.pack(pady=10)

        self.listado_habilidades_button = tk.Button(self, text="Listado de Habilidades",
                                                   command=self.mostrar_listado_habilidades_interface)
        self.listado_habilidades_button.pack(pady=10)

        self.personajes_button = tk.Button(self, text="Personajes", command=self.mostrar_personajes_interface)
        self.personajes_button.pack(pady=10)

    def mostrar_jugadores_interface(self):
        jugadores_interface = JugadoresInterface(self)
        jugadores_interface.grab_set()

    def mostrar_game_masters_interface(self):
        game_masters_interface = GameMastersInterface(self)
        game_masters_interface.grab_set()

    def mostrar_tabla_resumen_interface(self):
        tabla_resumen_interface = TablaResumenInterface(self)
        tabla_resumen_interface.grab_set()

    def mostrar_listado_habilidades_interface(self):
        listado_habilidades_interface = ListadoHabilidadesInterface(self)
        listado_habilidades_interface.grab_set()

    def mostrar_personajes_interface(self):
        personajes_interface = PersonajesInterface(self)
        personajes_interface.grab_set()


if __name__ == "__main__":
    app = MainInterface()
    app.mainloop()
