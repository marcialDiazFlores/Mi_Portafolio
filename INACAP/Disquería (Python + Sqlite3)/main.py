from BibliotecaMusica import *
from top20 import llenar_base_de_datos

def main():
    mi_biblioteca_musica = BibliotecaMusica("Biblioteca Musical")

    while True:
        print("\n1. Agregar Disco")
        print("2. Buscar Disco")
        print("3. Prestar Disco")
        print("4. Devolver Disco")
        print("5. Salir")

        opcion = input("Seleccione una opción: ")

        if opcion == '1':
            titulo = input("Ingrese el título del disco: ")
            artista = input("Ingrese el artista del disco: ")
            lanzamiento = int(input("Ingrese el año de lanzamiento del disco: "))
            mi_biblioteca_musica.agregar_disco(titulo, artista, lanzamiento)

        elif opcion == '2':
            titulo = input("Ingrese el título del disco a buscar: ")
            disco_encontrado = mi_biblioteca_musica.buscar_disco(titulo)
            if disco_encontrado:
                disco_encontrado.mostrar_info()
            else:
                print(f"No se encontró el disco con título '{titulo}'.")

        elif opcion == '3':
            nombre_persona = input("Ingrese el nombre de la persona: ")
            rut_persona = input("Ingrese el Rut de la persona: ")
            apellido_persona = input("Ingrese el apellido de la persona: ")

            mi_biblioteca_musica.agregar_persona(nombre_persona, rut_persona, apellido_persona)
            
            titulo_disco = input("Ingrese el título del disco a prestar: ")
            mi_biblioteca_musica.prestar_disco(titulo_disco, nombre_persona, rut_persona, apellido_persona)

        elif opcion == '4':
            rut_persona_devolucion = input("Ingrese el Rut de la persona que desea devolver un disco: ")
            pedidos_persona = mi_biblioteca_musica.obtener_pedidos_persona(rut_persona_devolucion)

            if not pedidos_persona:
                print("La persona no ha pedido discos aún.")
            else:
                print("Discos pedidos por la persona:")
                for idx, disco_pedido in enumerate(pedidos_persona, 1):
                    print(f"{idx}. {disco_pedido}")

                numero_disco_devolver = input("Ingrese el número del disco que desea devolver: ")

                if 1 <= int(numero_disco_devolver) <= len(pedidos_persona):
                    titulo_disco_devolver = pedidos_persona[int(numero_disco_devolver) - 1]
                    confirmacion_devolucion = input(f"Desea devolver el disco '{titulo_disco_devolver}'? Presione 1 para devolver el disco, 2 para cancelar: ")

                    if confirmacion_devolucion == '1':
                        mi_biblioteca_musica.devolver_disco(titulo_disco_devolver, rut_persona_devolucion)

        elif opcion == '5':
            break

llenar_base_de_datos()

if __name__ == "__main__":
    main()
