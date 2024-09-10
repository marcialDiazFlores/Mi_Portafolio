import sqlite3    

def llenar_base_de_datos():
    conexion = sqlite3.connect("biblioteca_musica.db")
    cursor = conexion.cursor()

    cursor.execute('DELETE FROM discos')
    cursor.execute('DELETE FROM personas')
    cursor.execute('DELETE FROM pedidos')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS discos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            titulo TEXT,
            artista TEXT,
            lanzamiento INTEGER,
            disponible BOOLEAN
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS personas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nombre TEXT,
            rut TEXT,
            apellido TEXT
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS pedidos (
            persona_id INTEGER PRIMARY KEY AUTOINCREMENT,
            disco_id INTEGER
        )
    ''')

    discos_mas_vendidos = [
        ("Thriller", "Michael Jackson", 1982),
        ("Back in Black", "AC/DC", 1980),
        ("The Dark Side of the Moon", "Pink Floyd", 1973),
        ("Bat Out of Hell", "Meat Loaf", 1977),
        ("Their Greatest Hits (1971–1975)", "Eagles", 1976),
        ("Saturday Night Fever", "Various Artists", 1977),
        ("Rumours", "Fleetwood Mac", 1977),
        ("The Bodyguard: Original Soundtrack Album", "Whitney Houston", 1992),
        ("Come On Over", "Shania Twain", 1997),
        ("Led Zeppelin IV", "Led Zeppelin", 1971),
        ("Jagged Little Pill", "Alanis Morissette", 1995),
        ("Bad", "Michael Jackson", 1987),
        ("Darkness on the Edge of Town", "Bruce Springsteen", 1978),
        ("The Wall", "Pink Floyd", 1979),
        ("Hotel California", "Eagles", 1976),
        ("21", "Adele", 2011),
        ("The Lion King: Original Motion Picture Soundtrack", "Various Artists", 1994),
        ("Back to Black", "Amy Winehouse", 2006),
        ("Abbey Road", "The Beatles", 1969),
        ("Sgt. Pepper's Lonely Hearts Club Band", "The Beatles", 1967),
    ]

    # Insertar registros en la tabla discos
    cursor.executemany(
        "INSERT INTO discos (titulo, artista, lanzamiento, disponible) VALUES (?, ?, ?, ?)",
        [
            (titulo, artista, lanzamiento, True)
            for titulo, artista, lanzamiento in discos_mas_vendidos
        ],
    )

    conexion.commit()
    conexion.close()

if __name__ == "__main__":
    llenar_base_de_datos()
