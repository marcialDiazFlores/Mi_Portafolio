from chofer import Chofer


class Bus:
    def __init__(self, numeroBus, capacidad, conductor=Chofer()):
        self.__numeroBus = numeroBus
        self.__capacidad = capacidad
        self.__conductor = conductor
        self.__listaPasajero = {}

    def __str__(self):
        return f"NumeroBus: {self.__numeroBus} Capacidad: {self.__capacidad} Conductor: {self.__conductor}"

    def getNumeroBus(self):
        return self.__numeroBus

    def getCapacidad(self):
        return self.__capacidad

    def getConductor(self):
        return self.__conductor

    def setNumeroBus(self, numeroBus):
        self.__numeroBus = numeroBus

    def setCapacidad(self, capacidad):
        self.__capacidad = capacidad

    def setConductor(self, conductor):
        self.__conductor = conductor


    def asignarChofer(self, conductor:Chofer):
        self.__conductor = conductor
        print("[+] El chofer se ha asignado exitosamente")
        print(self.__conductor)

    def asientosDisponibles(self):
        return self.__capacidad - len(self.__listaPasajero)

    def subirPasajero(self, pasajero):
        if self.asientosDisponibles() > 0:
            if not pasajero in self.__listaPasajero:
                self.__listaPasajero[pasajero.getRut()] = pasajero
                return "[+] Correcto, el pasajero ha sido subido al bus"
            return "[!] Error al Subir, el pasajero ya se encuentra ingresado!!"
        return "[!] Error al subir, Bus vendido en su totalidad, no se puede ingresar otro pasajero"

    def bajarPasajero(self, rut):
        if len(self.__listaPasajero):
            if rut in self.__listaPasajero:
                del self.__listaPasajero[rut]
                return "[+] Correcto, El pasajero ha sido eliminado de la lista"
            return "[!] El Pasajero no se encuentra en el bus!!"
        
        return "[!] Lo Sentimos, Bus sin pasajeros"

    def desplegaraPasajeros(self):
        print(self)
        if len(self.__listaPasajero)>0:
            print("Esta es la lista de pasajeros: ")
            for rut in self.__listaPasajero:
                print(self.__listaPasajero[rut])
            return True
        print("La lista de pasajeros esta vacia!!")