import sys

#Contrato: int int -> int
#Chequea si los valores ingresados son positivos, y retorna el valor total de dinero inicial.
#Ejemplos: saldoInicial(3,2) retorna 32000, saldoInicial(-3,5) muestra mensaje de error y cierra el programa.
#Cuerpo de la funcion:
def saldoInicial(inicial10000,inicial1000):
    #Si se ingresan parametros menores a 0, arroja error y termina el programa:
    if inicial10000<0 or inicial1000<0:
        print ("Error: El programa no acepta cantidad negativa de billetes. Se cerrar� el programa :C")
        sys.exit(0)
    #Si los valores son positivos calcula el monto total:    
    else:
       return inicial10000*10000+inicial1000*1000
#Tests:
assert saldoInicial(3,2)==32000
assert saldoInicial(2,12)==32000
assert saldoInicial(0,0)==0

#Contrato: int int int -> none
#Efectua una cantidad noper (entera y positiva) de operaciones, siendo deposito y giro las operaciones disponibles.
#billetes10k y billetes1k son el saldo en billetes de 10000 y 1000 respectivamente, al momento de llamar la funcion (inicial o recursivamente).
#Ejemplos: operaciones(3,10,0) efectua 3 operaciones con un saldo inicial de 10 billetes de 10000, operaciones(6,12,30) efectua 6 operaciones
#con un saldo inicial de 12 billetes de 10000 y 30 billetes de 1000.
#Cuerpo de la funcion:
def operaciones(noper,billetes10k,billetes1k):
    #Si noper es negativo arroja error y termina el programa:
    if noper<0:
        print ("Error: El programa no acepta cantidad negativa de operaciones. Se cerrar� el programa :C")
        sys.exit(0)
    #Caso base (noper==0):    
    elif noper==0:
        return
    #Caso recursivo:
    else:
        operacion=input("Que operacion desea realizar? ")
        if operacion=="giro":
            saldoactual10k,saldoactual1k = giro(billetes10k,billetes1k)
        elif operacion=="deposito":
            saldoactual10k,saldoactual1k = deposito(billetes10k,billetes1k)
        else:
            print ("Error: La operaci�n ingresada no es v�lida. Se cerrar� el programa. :C")
            sys.exit(0)
        operaciones(noper-1,saldoactual10k,saldoactual1k)

#Contrato: int int -> int int
#Efectua un deposito, siendo billetes10k y billetes1k la cantidad de billetes de 10000 y 1000 al llamar la funcion respectivamente;
#luego retorna las cantidades de billetes de 10000 y 1000 actualizadas.
#Ejemplos: ingresar deposito(10,2), luego 1 y 2, retorna 11 billetes de 10000 y 4 de 1000 (11,4);
#ingresar deposito(-4,3) arroja error, ingresar deposito(10,10), luego 0 y 0, retorna 10 billetes de 10000 y 1000 (10,10).
#Cuerpo de la funcion:
def deposito(billetes10k,billetes1k):
    billetesDeposito10k=int(input("Cuantos billetes de 10000 desea depositar? "))
    billetesDeposito1k=int(input("Cuantos billetes de 1000 desea depositar? "))
    #Si se ingresan cantidades negativas para el deposito arroja error y termina el programa:
    if billetes10k<0 or billetes1k<0:
        print ("Error: El programa no acepta cantidad negativa de billetes. Se cerrar� el programa. :C")
        sys.exit(0)
    #Si los valores son positivos se efectua el deposito:
    else:
        deposito=billetesDeposito10k*10000+billetesDeposito1k*1000
        print ("Deposito de $"+str(deposito))
        saldoActual10k=billetes10k+billetesDeposito10k
        saldoActual1k=billetes1k+billetesDeposito1k
        print ("Saldo actual en la caja: "+str(saldoActual10k)+" de $10000 y "+str(saldoActual1k)+" de $1000.")
        return saldoActual10k,saldoActual1k

#Contrato: int int -> int int
#Efectua un giro, siendo billetes10k y billetes1k la cantidad de billetes disponibles de 10000 y 1000 al llamar la funcion respectivamente;
#luego retorna las cantidades de billetes de 10000 y 1000 actualizadas. Si no se puede hacer el giro (falta de billetes) se entrega un
#mensaje de giro rechazado y se continua con la siguiente operacion.
#Ejemplos: ingresar giro(2,4), luego 30000 rechaza el giro (falta dinero); ingresar giro(3,2), luego 30000 retorna 0 billetes de 10000 y 2 de 1000 (0,2);
#ingresar giro(3,2) y luego 25000 rechaza el giro (ninguna combinacion de billetes cumple; ingresar giro(1,3) y luego -32000 arroja error (cantidad negativa).
#Cuerpo de la funci�n:
def giro(billetes10k,billetes1k):
    montoGiro=int(input("Que monto desea girar? "))
    saldoActual=billetes10k*10000+billetes1k*1000
    #Si el monto del giro es negativo arroja error:
    if montoGiro<0:
       print ("Error: El programa no acepta monto negativo para el giro. Se cerrar� el programa. :C")
       sys.exit(0)
    #Si el monto de giro supera el saldo se rechaza el giro:
    elif montoGiro>saldoActual:
        print ("Giro de $"+str(montoGiro)+" rechazado. :C")
        print ("Saldo: "+str(billetes10k)+" de $10000 y "+str(billetes1k)+" de $1000.")
        return billetes10k,billetes1k
    #Si el monto de giro es multiplo de 10 y menor que el saldo que tenemos en billetes de 10000, se paga solo con billetes de 10000:
    elif montoGiro%10000==0 and billetes10k>=montoGiro/10000:
        print ("Giro de $"+str(montoGiro)+". Saca "+str(montoGiro/10000)+" de $10000 y 0 de $1000")
        saldoActual10k=billetes10k-montoGiro/10000
        saldoActual1k=billetes1k
        print ("Saldo: "+str(saldoActual10k)+" de $10000 y "+str(saldoActual1k)+" de $1000.")
        return saldoActual10k,saldoActual1k
    #Si el monto de giro se puede pagar sacando todos los billetes de 10000 disponibles, mas una diferencia en billetes de 1000:
    elif billetes10k<montoGiro/10000 and montoGiro-billetes10k*10000<=billetes1k*1000:
        diferencia1k=(montoGiro-billetes10k*10000)/1000
        print ("Giro de $"+str(montoGiro)+". Saca "+str(billetes10k)+" de $10000 y "+str(diferencia1k)+" de $1000")
        saldoActual10k=0
        saldoActual1k=billetes1k-diferencia1k
        print ("Saldo: "+str(saldoActual10k)+" de $10000 y "+str(saldoActual1k)+" de $1000.")
        return saldoActual10k,saldoActual1k
    #Si no tenemos suficientes billetes (quizas esto sobra):
    elif billetes10k<montoGiro/10000 and montoGiro-billetes10k*10>billetes1k*1000:
        print ("Giro de $"+str(montoGiro)+" rechazado. :C")
        print ("Saldo: "+str(billetes10k)+" de $10000 y "+str(billetes1k)+" de $1000.")
        return billetes10k,billetes1k
    #Si el monto de giro no es multiplo de 1000 se rechaza:
    elif montoGiro%1000!=0:
        print ("Giro de $"+str(montoGiro)+" rechazado. :C")
        print ("Saldo: "+str(billetes10k)+" de $10000 y "+str(billetes1k)+" de $1000.")
        return billetes10k,billetes1k
    #Si el monto de giro es menor al saldo, y se puede pagar con los dos tipos de billetes:
    elif billetes10k>=montoGiro/10000 and montoGiro%10000<=billetes1k*1000:
        print ("Giro de $"+str(montoGiro)+". Saca "+str(montoGiro//10000)+" de $10000 y "+str((montoGiro%10000)//1000)+" de $1000")
        saldoActual10k=billetes10k-montoGiro//10000
        saldoActual1k=billetes1k-(montoGiro%10000)//1000
        print ("Saldo: "+str(saldoActual10k)+" de $10000 y "+str(saldoActual1k)+" de $1000.")
        return saldoActual10k,saldoActual1k
    #Cualquier otro caso se rechaza:
    else:
        print ("Giro de $"+str(montoGiro)+" rechazado. :C")
        print ("Saldo: "+str(billetes10k)+" de $10000 y "+str(billetes1k)+" de $1000.")
        return billetes10k,billetes1k

#Presionar F5 para empezar a usar el programa.

print ("Bienvenide al simulador de cajero")
inicial10000=int(input("Saldo inicial de 10000? (En billetes) "))
inicial1000=int(input("Saldo inicial de 1000? (En billetes) "))
totalInicial=saldoInicial(inicial10000,inicial1000)
print ("$"+str(totalInicial))
noper=int(input("Cuantas operaciones desea realizar? "))
operaciones(noper,inicial10000,inicial1000)
print ("Operaciones terminadas. Gracias por usar el simulador de cajero! c:")

