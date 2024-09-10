from bus import Bus
from chofer import Chofer
from pasajero import Pasajero

##--------- INGRESOS Y VALIDACIONES ---------

def ingresar_opcion():
    opcion = input("Ingrese una opcion: ")
    return opcion

def ingresar_capacidad():
    try:
        capacidad = int(input("Ingrese la capacidad del Bus: "))
        return capacidad
    except ValueError:
        print("[!] Error, La cantidad debe ser un numero")
        return ingresar_capacidad()

def ingresar_numero():
    try:
        numeroBus = int(input("Ingrese el numero del Bus: "))
        return numeroBus
    except ValueError:
         print("[!] Error, La cantidad debe ser un numero")
         return ingresar_numero()

def ingresar_rut():
    rut = input("Ingrese el Rut: ")
    return rut

def ingresar_nombre():
    nombre = input("Ingrese el Nombre: ")
    return nombre

def ingresar_edad():
    edad = input("Ingrese la edad: ")
    return edad

def ingresar_antiguedad():
    antiguedad = input("Ingrese la antiguedad: ")
    return antiguedad

##------------ FUNCIONES MENU ----------------------

def ingresar_bus():
    global bus
    bus = Bus(ingresar_numero(), ingresar_capacidad())
    return bus

def asignar_chofer(bus):
    conductor = Chofer(ingresar_rut(), ingresar_numero(), ingresar_antiguedad())
    bus.asignarChofer(conductor)

def subir_pasajero(bus):
    pasajero = Pasajero(ingresar_rut(), ingresar_nombre(), ingresar_edad())
    print(bus.subirPasajero(pasajero))

def bajar_pasajero(bus):
    print(bus.bajarPasajero(ingresar_rut()))

def menu():
    print("\n----------- MENU PRINCIPAL ------------\n")
    print("[1] Ingresar Bus")
    print("[2] Asignar Conductor")
    print("[3] Subir Pasajeros")
    print("[4] Bajar Pasajeros")
    print("[5] Listar Pasajeros")
    print("[6] Asientos disponibles")
    print("[7] Salir")
    print("\n--------------------------------------\n")

sw=0

while True:
    menu()
    opcion = ingresar_opcion()

    if opcion == "1":
        ingresar_bus()
        sw=1
    elif opcion == "2" and sw==1:
        asignar_chofer(bus)
    elif opcion == "3" and sw==1:
        subir_pasajero(bus)
    elif opcion == "4" and sw==1:
        bajar_pasajero(bus)
    elif opcion == "5" and sw==1:
        bus.desplegaraPasajeros()
    elif opcion == "6" and sw==1:
        print("Asientos disponibles: ",bus.asientosDisponibles())
    elif opcion == "7":
        exit("Saliendo...")
    else: 
        print("[!] Error, La opcion ingresada es invalida!!, o ya existe el bus")