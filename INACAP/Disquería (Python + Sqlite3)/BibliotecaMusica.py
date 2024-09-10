from Disco import *
from Persona import *
import sqlite3

class BibliotecaMusica:
    def __init__(self, nombre):
        self.nombre = nombre
        self.conexion = sqlite3.connect('biblioteca_musica.db')
        self.crear_tabla()

    def crear_tabla(self):
        cursor = self.conexion.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS discos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                titulo TEXT,
                artista TEXT,
                lanzamiento INTEGER,
                disponible BOOLEAN
            )
        ''')
        self.conexion.commit()

    def agregar_disco(self, titulo, artista, lanzamiento):
        cursor = self.conexion.cursor()
        cursor.execute('INSERT INTO discos (titulo, artista, lanzamiento, disponible) VALUES (?, ?, ?, ?)', (titulo, artista, lanzamiento, True))
        self.conexion.commit()

    def buscar_disco(self, titulo):
        cursor = self.conexion.cursor()
        cursor.execute('SELECT * FROM discos WHERE titulo = ?', (titulo,))
        disco_data = cursor.fetchone()
        if disco_data:
            return Disco(titulo=disco_data[1], artista=disco_data[2], lanzamiento=disco_data[3], disponible=disco_data[4], id=disco_data[0])
        else:
            return None

    def agregar_persona(self, nombre, rut, apellido):
        cursor = self.conexion.cursor()
        cursor.execute('INSERT INTO personas (nombre, rut, apellido) VALUES (?, ?, ?)', (nombre, rut, apellido))
        self.conexion.commit()

    def obtener_persona(self, rut):
        cursor = self.conexion.cursor()
        cursor.execute('SELECT * FROM personas WHERE rut = ?', (rut,))
        persona_data = cursor.fetchone()
        if persona_data:
            return Persona(persona_data[1], persona_data[2], persona_data[3])
        else:
            return None

    def prestar_disco(self, titulo, nombre_persona, rut_persona, apellido_persona):
        persona = self.obtener_persona(rut_persona)

        if not persona:
            self.agregar_persona(nombre_persona, rut_persona, apellido_persona)
            persona = self.obtener_persona(rut_persona)

        disco = self.buscar_disco(titulo)

        if disco and persona:
            cursor = self.conexion.cursor()

            # Insertar la persona si aún no tiene un id asignado
            if persona.id is None:
                cursor.execute('INSERT INTO personas (nombre, rut, apellido) VALUES (?, ?, ?)', (persona.nombre, persona.rut, persona.apellido))
                persona.id = cursor.lastrowid  # Asignar el id generado

            # Insertar el pedido
            cursor.execute('INSERT INTO pedidos (persona_id, disco_id) VALUES (?, ?)', (persona.id, disco.id))
            cursor.execute('UPDATE discos SET disponible = 0 WHERE id = ?', (disco.id,))
            
            self.conexion.commit()
            print(f"Disco '{titulo}' prestado a {persona.nombre} {persona.apellido} con éxito.")
        else:
            print("Error: Disco o persona no encontrados.")

    def obtener_pedidos_persona(self, rut):
        cursor = self.conexion.cursor()
        cursor.execute('''
            SELECT discos.titulo
            FROM pedidos
            INNER JOIN discos ON pedidos.disco_id = discos.id
            INNER JOIN personas ON pedidos.persona_id = personas.id
            WHERE personas.rut = ?
        ''', (rut,))
        pedidos = cursor.fetchall()
        return [pedido[0] for pedido in pedidos]

    def devolver_disco(self, titulo, rut_persona):
        persona = self.obtener_persona(rut_persona)
        disco = self.buscar_disco(titulo)

        if disco and persona:
            cursor = self.conexion.cursor()
            cursor.execute('DELETE FROM pedidos WHERE persona_id = ? AND disco_id = ?', (persona.id, disco.id))
            cursor.execute('UPDATE discos SET disponible = 1 WHERE id = ?', (disco.id,))
            self.conexion.commit()
            print(f"Disco '{titulo}' devuelto por {persona.nombre} {persona.apellido} con éxito.")
        else:
            print("Error: Disco o persona no encontrados.")
