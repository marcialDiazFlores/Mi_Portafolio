#!/usr/bin/env python
# -*- coding: utf-8 -*-
import estructura
from lista import *

# esListaStr: lista -> bool
# Verifica que los elementos de una lista sean del tipo string,
# si algun valor no es str retorna False, y retorna True para listas vacias y
# listas con valores del tipo str:
# Ej: esListaStr(listaVacia) retorna True, esListaStr(lista(3, listaVacia)) retorna
# False, y esListaStr(lista("Juan", lista("Diego", lista("Pedro", listaVacia)))) retorna
# True.
# Cuerpo de la funcion:
def esListaStr(unaLista):
    assert esLista(unaLista)
    if unaLista == listaVacia:
        return True
    else:
        if type(cabeza(unaLista)) == str:
            return esListaStr(cola(unaLista))
        else:
            return False
# Tests:
assert esListaStr(lista("Juan", lista("Diego", lista("Pedro", listaVacia))))
assert esListaStr(lista("h", listaVacia))
assert esListaStr(listaVacia)
assert not esListaStr(lista(3, lista("hola", lista(True, listaVacia))))

# Diseño de la estructura:
# humano: Nombre(Str) Genero(Str) Edad(Int) Amigos(Lista de Strings)
estructura.crear("humano", "Nombre Genero Edad Amigos")

# Humanos y redes a usar en los tests:
Juan = humano("Juan", "M", 17, listaVacia)
Alberto = humano("Alberto", "M", 64, lista("Juan", listaVacia))
Ben = humano("Ben", "M", 34, listaVacia)
Elisa = humano("Elisa", "F", 16, lista("Juan", lista("Alberto", listaVacia)))
Mirta = humano("Mirta", "F", 67, lista("Alberto", lista("Elisa", listaVacia)))
redO = listaVacia
redX = lista(Ben, redO)
redY = lista(Juan, redX)
redZ = lista(Alberto, redY)
redFinal = lista(Mirta, lista(Elisa, redZ))


# crearRedSocial: None -> lista
# Crea una lista vacia, que sera la primera red social.
# Ej: crearRedSocial() retorna listaVacia
# Cuerpo de la funcion:
def crearRedSocial():
    redA = listaVacia
    return redA
# Tests:
assert crearRedSocial() == listaVacia

# esHumano: humano -> bool
# Comprueba si el valor ingresado es del tipo humano.
# Si es humano retorna True, en caso contrario arroja error.
# Ej: esHumano(humano("Juan", "M", 17, listaVacia)) retorna True,
# esHumano(humano(12, "Martin", listaVacia, 23)) arroja error.
# Cuerpo de la funcion:
def esHumano(unHumano):
    assert type(unHumano) == humano
    assert type(unHumano.Nombre) == str
    assert unHumano.Genero == "M" or unHumano.Genero == "F"
    assert type(unHumano.Edad) == int and unHumano.Edad >= 0
    assert esListaStr(unHumano.Amigos)
    return True
# Tests:
assert esHumano(Juan)
assert esHumano(Alberto)
assert esHumano(Ben)

# esRedSocial: lista -> bool
# Verifica que todos los elementos de la lista ingresada (red) sean del
# tipo humano. Retorna True para listas vacias y de humanos, False para
# el resto.
# Ej: esRedSocial(lista(humano("Juan", "M", 17, listaVacia), lista(humano("Alberto", "M", 64, lista("Juan", listaVacia)), listaVacia)))
# retorna True, esRedSocial(lista(humano("Juan", "M", 17, listaVacia), lista(3, lista("hola", listaVacia)))) retorna False.
def esRedSocial(unaRed):
    assert esLista(unaRed)
    if unaRed == listaVacia:
        return True
    else:
        if type(cabeza(unaRed)) == humano:
            return esRedSocial(cola(unaRed))
        else:
            return False
# Tests:
assert esRedSocial(redX)
assert esRedSocial(listaVacia)
assert esRedSocial(redZ)
assert not esRedSocial(lista(Juan, lista(3, lista("hola", listaVacia))))

# unirHumano: humano lista -> lista
# Agrega al humano a la red social, y retorna la red actualizada.
# Ej: unirHumano(humano("Juan", "M", 17, listaVacia), crearRedSocial()) retorna lista(humano("Juan", "M", 17, listaVacia), listaVacia),
# unirHumano(humano("Alberto", "M", 64, lista("Juan", listaVacia)), lista(humano("Juan", "M", 17, listaVacia), listaVacia))
# retorna lista(humano("Alberto", "M", 64, lista("Juan", listaVacia)), lista(humano("Juan", "M", 17, listaVacia), listaVacia))
def unirHumano(unHumano, unaRed):
    assert esHumano(unHumano)
    assert esRedSocial(unaRed)
    nuevaRed = crearLista(unHumano, unaRed)
    assert esRedSocial(nuevaRed)
    return nuevaRed
# Tests:
assert unirHumano(Ben, crearRedSocial()) == redX
assert unirHumano(Juan, redX) == redY
assert unirHumano(Alberto, redY) == redZ

# buscarHumano: str lista -> None/humano
# Busca un humano (ingresado como el string de su nombre) en la red. Si el humano a buscar
# no esta en la red, retorna None, en caso contrario retorna a dicho humano.
# Ej: buscarHumano("Alberto", redZ) retorna a Alberto, buscarHumano("Juan", redO) retorna None.
# Cuerpo de la funcion:
def buscarHumano(unHumano, unaRed):
    assert type(unHumano) == str and esRedSocial(unaRed)
    if esListaVacia(unaRed):
        return None
    else:
        primerHumano = cabeza(unaRed)
        if unHumano == primerHumano.Nombre:
            return primerHumano
        else:
            return buscarHumano(unHumano, cola(unaRed))
# Tests:
assert buscarHumano("Alberto", redZ) == Alberto
assert buscarHumano("Ben", redZ) == Ben
assert buscarHumano("Juan", redO) == None
assert buscarHumano("Marcelo", redY) == None

# eliminarHumano: str lista -> lista
# Elimina al humano cuyo nombre es el ingresado (como string) de la red social,
# y retorna la red sin dicho humano. Si el humano no esta en la red social, no se 
# produce ningun cambio en esta.
# Ej: eliminarHumano("Alberto", redZ) retorna redY, eliminarHumano("Ben", redY)
# retorna lista(Juan, listaVacia)
# Cuerpo de la funcion:
def eliminarHumano(unHumano, unaRed):
    assert type(unHumano) == str
    assert esRedSocial(unaRed)
    if esListaVacia(unaRed):
        return unaRed
    else:
        primerHumano = cabeza(unaRed)
        if primerHumano.Nombre == unHumano:
            return cola(unaRed)
        else:
            return lista(cabeza(unaRed), eliminarHumano(unHumano, cola(unaRed)))
# Tests:
assert eliminarHumano("Alberto", redZ) == redY
assert eliminarHumano("Ben", redY) == lista(Juan, listaVacia)
assert eliminarHumano("Martin", redY) == redY

# buscarEnAmigos: humano lista -> bool
# Verifica si un humano esta en la lista de amigos de otro humano (ingresada como lista de strings).
# Si el humano esta en la lista retorna True, en caso contrario retorna False.
# Ej: buscarEnAmigos(Ben, Elisa.Amigos) retorna True, buscarEnAmigos(Juan, Mirta.Amigos) retorna False.
# Cuerpo de la funcion:
def buscarEnAmigos(unHumano, listaAmigos):
    assert esHumano(unHumano) and esListaStr(listaAmigos)
    if listaAmigos == listaVacia:
        return False
    elif unHumano.Nombre == cabeza(listaAmigos):
        return True
    else:
        return buscarEnAmigos(unHumano, cola(listaAmigos))
# Tests:
assert buscarEnAmigos(Juan, Alberto.Amigos)
assert buscarEnAmigos(Juan, Elisa.Amigos)
assert not buscarEnAmigos(Juan, Mirta.Amigos)
assert not buscarEnAmigos(Ben, Alberto.Amigos)

# agregarAmigos: str str lista -> lista/None
# Recibe los nombres de dos humanos y una red social, para luego retornar otra red social, en la cual
# los dos humanos estaran en sus lista de amigos. Si se ingresan humanos que no pertenecen a la red ingresada
# muestra un mensaje en pantalla como aviso y la funcion retorna None. 
# Si los humanos ya eran amigos, se retorna None.
# Ej: agregarAmigos("Juan", "Ben", redY) retorna una red donde Juan aparece en la lista de amigos de Ben y viceversa.
# agregarAmigos("Juan", "Ben", redFinal) retorna None, agregarAmigos("Javier", "Ben", redFinal) muestra en pantalla
# el mensaje "Javier no pertenece a la red ingresada" y retorna None.
# Cuerpo de la funcion:
def agregarAmigos(humanoUno, humanoDos, unaRed):
    assert type(humanoUno) == str and type(humanoDos) == str
    assert esRedSocial(unaRed)
    primerHumano = buscarHumano(humanoUno, unaRed)
    segundoHumano = buscarHumano(humanoDos, unaRed)
    if buscarHumano(humanoUno, unaRed) == None and buscarHumano(humanoDos, unaRed) == None:
        print (humanoUno + " y " + humanoDos + " no pertenecen a la red ingresada")
        return 
    elif buscarHumano(humanoUno, unaRed) == None:
        print (humanoUno + " no pertenece a la red ingresada")
        return 
    elif buscarHumano(humanoDos, unaRed) == None:
        print (humanoDos + " no pertenece a la red ingresada")
        return 
    elif buscarEnAmigos(primerHumano, segundoHumano.Amigos) and buscarEnAmigos(segundoHumano, primerHumano.Amigos):
        return 
    elif buscarEnAmigos(primerHumano, segundoHumano.Amigos):
        amigosHumanoUno = lista(segundoHumano.Nombre, primerHumano.Amigos)
        nuevoHumanoUno = humano(primerHumano.Nombre, primerHumano.Genero, primerHumano.Edad, amigosHumanoUno)
        nuevoHumanoDos = segundoHumano
        segundaRed = eliminarHumano(humanoUno, unaRed)
        terceraRed = eliminarHumano(humanoDos, segundaRed)
        cuartaRed = unirHumano(nuevoHumanoUno, terceraRed)
        redActualizada = unirHumano(nuevoHumanoDos, cuartaRed)
        return redActualizada
    elif buscarEnAmigos(segundoHumano, primerHumano.Amigos):
        amigosHumanoDos = lista(primerHumano.Nombre, segundoHumano.Amigos)
        nuevoHumanoDos = humano(segundoHumano.Nombre, segundoHumano.Genero, segundoHumano.Edad, amigosHumanoDos)
        nuevoHumanoUno = primerHumano
        segundaRed = eliminarHumano(humanoUno, unaRed)
        terceraRed = eliminarHumano(humanoDos, segundaRed)
        cuartaRed = unirHumano(nuevoHumanoUno, terceraRed)
        redActualizada = unirHumano(nuevoHumanoDos, cuartaRed)
        return redActualizada
    else:
        amigosHumanoUno = lista(segundoHumano.Nombre, primerHumano.Amigos)
        amigosHumanoDos = lista(primerHumano.Nombre, segundoHumano.Amigos)
        nuevoHumanoUno = humano(primerHumano.Nombre, primerHumano.Genero, primerHumano.Edad, amigosHumanoUno)
        nuevoHumanoDos = humano(segundoHumano.Nombre, segundoHumano.Genero, segundoHumano.Edad, amigosHumanoDos)
        segundaRed = eliminarHumano(humanoUno, unaRed)
        terceraRed = eliminarHumano(humanoDos, segundaRed)
        cuartaRed = unirHumano(nuevoHumanoUno, terceraRed)
        redActualizada = unirHumano(nuevoHumanoDos, cuartaRed)
        return redActualizada

# listaNombres: lista -> lista
# Retorna una lista con los nombres de las personas pertenecientes a la red social ingresada.
# Si se ingresa una lista vacia, retorna la lista ingresada.
# Ej: listaNombres(listaVacia) retorna listaVacia, listaNombres(redX) retorna lista("Ben", listaVacia).
# listaNombres(redY) retorna lista("Juan", lista("Ben", listaVacia))
# Cuerpo de la funcion:
def listaNombres(unaRed):
    assert esRedSocial(unaRed)
    if esListaVacia(unaRed):
        return unaRed
    else:
        primerHumano = cabeza(unaRed)
        nombre = primerHumano.Nombre
        return lista(nombre, listaNombres(cola(unaRed)))
# Tests:
assert listaNombres(redY) == lista("Juan", lista("Ben", listaVacia))
assert listaNombres(redZ) == lista("Alberto", lista("Juan", lista("Ben", None)))
assert listaNombres(redX) == lista("Ben", listaVacia)

# listaToStr: lista -> str
# Retorna un string compuesto por los elementos de la lista ingresada, separados por un guion.
# Ej: listaToStr(listaVacia) retorna "", listaToStr(listaNombres(redZ)) retorna "Alberto - Juan - Ben"
# Cuerpo de la funcion:
def listaToStr(L):
    assert esLista(L)
    if esListaVacia(L):
        return ""
    if cola(L) == listaVacia:
        return str(cabeza(L))
    return str(cabeza(L)) +" - "+ listaToStr(cola(L))
#Tests
assert listaToStr(listaNombres(redZ)) == "Alberto - Juan - Ben"
assert listaToStr(listaNombres(redX)) == "Ben"
assert listaToStr(listaVacia) == ""

# mostrarHumanos: lista -> None
# Muestra un string en pantalla con los nombres de los integrantes de la red social,
# separados por guiones.
# Ej: mostrarHumanos(redFinal) muestra el mensaje Integrantes de TweetCat:
# Mirta - Elisa - Juan - Ben - Alberto
def mostrarHumanos(unaRed):
    assert esRedSocial(unaRed)
    nombres = listaNombres(unaRed)
    humanos = listaToStr(nombres)
    print ("Integrantes de TweetCat:")
    print (humanos)
    return

# mostrarAmigos: str lista -> None
# Muestra en pantalla un string con los nombres de los amigos del humano ingresado,
# en la red social ingresada. Si el humano ingresado no pertenece a la red ingresada,
# muestra un mensaje diciendolo.
# Ej: mostrarAmigos("Elisa", redFinal) muestra el mensaje Amigos de Elisa: Juan - Alberto
# y mostrarAmigos("Harry", redFinal) muestra el mensaje Harry no pertenece a la red ingresada.
# Cuerpo de la funcion:
def mostrarAmigos(unHumano, unaRed):
    assert type(unHumano) == str and esRedSocial(unaRed)
    if buscarHumano(unHumano, unaRed) == None:
        print (unHumano + " no pertenece a la red ingresada")
        return
    else:
        humano = buscarHumano(unHumano, unaRed)
        listaAmigos = humano.Amigos
        if esListaVacia(listaAmigos):
            print ("Amigos de " + humano.Nombre + ": No tiene amigos aun")
            return
        else:
            amigos = listaToStr(listaAmigos)
            print ("Amigos de " + humano.Nombre + ": " + amigos)
            return

# filtro: lista int str -> None/lista
# Recibe como parametros una red social, un entero que representa la edad y un string("M" o "F") que
# representa el genero. Si la red ingresada esta vacia, retorna listaVacia (None). En caso contrario
# entrega la lista filtrada, es decir, con los humanos del genero ingresado y de edad mayor o igual a la ingresada.
# Ej: filtro(redFinal, 30, "F") retorna lista(Mirta, listaVacia), filtro(redFinal, 27, "M") retorna
# lista(Alberto, lista(Ben, listaVacia)) y filtro(redFinal, 100, "M") retorna listaVacia.
# Cuerpo de la funcion:
def filtro(unaRed, edad, genero):
    assert esRedSocial(unaRed) and type(edad) == int and type(genero) == str
    primerHumano = cabeza(unaRed)
    if esListaVacia(unaRed):
        return listaVacia
    elif primerHumano.Genero == genero and primerHumano.Edad >= edad:
        return lista(primerHumano, filtro(cola(unaRed), edad, genero))
    else:
        return filtro(cola(unaRed), edad, genero)
# Tests:
assert filtro(redFinal, 30, "F") == lista(Mirta, listaVacia)
assert filtro(redFinal, 27, "M") == lista(Alberto, lista(Ben, listaVacia))
assert filtro(redFinal, 100, "M") == listaVacia

# filtrar: lista int str -> None
# Muestra en pantalla un string, con los nombres de las personas contenidas en la lista resultante.
# de usar la funcion filtro con los parametros ingresados para filtrar.
# Si la lista filtrada es vacia, muestra el mensaje "No existen humanos que cumplan esos criterios".
# Si se ingresa un string que no sea "M" o "F" para el genero, se muestra el mensaje "El string ingresado para el genero no es valido".
# Ej: filtrar(redFinal, 30, "F") muestra en pantalla el mensaje "Mujeres mayores de 30: 
# Mirta" y filtrar(redFinal, 100, "M") muestra en pantalla el mensaje Hombres mayores de 100:
# No existen humanos que cumplan esos criterios.
# Cuerpo de la funcion:
def filtrar(unaRed, edad, genero):
    assert esRedSocial(unaRed) and type(edad) == int and type(genero) == str
    redFiltrada = filtro(unaRed, edad, genero)
    nombres = listaNombres(redFiltrada)
    if esListaVacia(redFiltrada):
        if genero == "M":
            print("Hombres mayores de " + str(edad) + ":")
            print("No existen humanos que cumplan esos criterios")  
            return
        elif genero == "F":
            print("Mujeres mayores de " + str(edad) + ":")
            print("No existen humanos que cumplan esos criterios")  
            return
        else:
            print("El string ingresado para el genero no es valido")
            return
    elif genero == "M":
        print("Hombres mayores de " + str(edad) + ":")
        print(listaToStr(nombres))
        return
    elif genero == "F":
        print("Mujeres mayores de " + str(edad) + ":")
        print(listaToStr(nombres))
        return
    else:
        print("El string ingresado para el genero no es valido")
        return


# Diseño de la estructura Nodo:
# Nodo: Nombre(string) Mensaje(string)
estructura.crear("Nodo", "Nombre Mensaje")

# Nodos y timelines para los tests:
nodoA = Nodo("Juan", "Hola")
nodoB = Nodo("Ben", "Como estan?")
nodoC = Nodo("Juan", "Sukistrukis")
nodoD = Nodo("Alberto", "Nanana")
tlA = listaVacia
tlB = lista(nodoA, listaVacia)
tlC = lista(nodoB, tlB)
tlD = lista(nodoC, tlC)
tlE = lista(nodoD, tlD) 

# esTimeline: lista -> bool
# Verifica que una lista sea timeline (lista de nodos o lista vacia). De ser asi retorna True, en caso
# contrario retorna False.
# Ej: esTimeline(tlC) retorna True, esTimeline(listaVacia) retorna True, y esTimeline(lista(2, listaVacia)) retorna False.
# Cuerpo de la funcion:
def esTimeline(unTimeline):
    assert esLista(unTimeline)
    if esListaVacia(unTimeline):
        return True
    else:
        if type(cabeza(unTimeline)) == Nodo:
            return esTimeline(cola(unTimeline))
        else:
            return False
# Tests:
assert esTimeline(tlC)
assert esTimeline(listaVacia)
assert not esTimeline(lista(2, listaVacia))

# buscarNodos: str lista -> None/lista
# Busca nodos con el nombre del humano ingresado en el timeline ingresado, y retorna una
# lista de estos nodos.
# Ej: buscarNodos("Juan", tlD) retorna lista(Nodo("Juan", "Sukistrukis"), lista(Nodo("Juan", "Hola"), listaVacia))
# y buscarNodos("Elisa", tlD) retorna None
def buscarNodos(unHumano, timeline):
    assert type(unHumano) == str and esTimeline(timeline)
    if esListaVacia(timeline):
        return None
    else:
        primerNodo = cabeza(timeline)
        if primerNodo.Nombre == unHumano:
            return lista(primerNodo, buscarNodos(unHumano, cola(timeline)))
        else:
            return buscarNodos(unHumano, cola(timeline))
# Tests:
assert buscarNodos("Juan", tlD) == lista(Nodo("Juan", "Sukistrukis"), lista(Nodo("Juan", "Hola"), listaVacia))
assert buscarNodos("Elisa", tlD) == None

# mensajeNodos: lista -> lista
# Recibe una lista de Nodos y retorna una lista con los mensajes contenidos en cada Nodo.
# Ej: mensajeNodos(lista(Nodo("Juan", "Sukistrukis"), lista(Nodo("Juan", "Hola"), listaVacia))) retorna lista("Sukistrukis", lista("Hola", listaVacia))
# y mensajeNodos(listaVacia) retorna listaVacia.
# Cuerpo de la funcion:
def mensajeNodos(listaNodos):
    assert esTimeline(listaNodos)
    if esListaVacia(listaNodos):
        return listaVacia
    else:
        primerNodo = cabeza(listaNodos)
        primerMensaje = primerNodo.Mensaje
        return lista(primerMensaje, mensajeNodos(cola(listaNodos)))
# Tests:
assert mensajeNodos(lista(Nodo("Juan", "Sukistrukis"), lista(Nodo("Juan", "Hola"), listaVacia))) == lista("Sukistrukis", lista("Hola", listaVacia))
assert mensajeNodos(listaVacia) == listaVacia

# printMensajes: lista -> None
# Muestra uno por uno en pantalla los mensajes obtenidos al usar la funcion mensajeNodos.
# Ej: printMensajes(lista("Sukistrukis", lista("Hola", listaVacia))) muestra en pantalla el mensaje
# Sukistrukis Hola, printMensajes(listaVacia) retorna None.
# Cuerpo de la funcion:
def printMensajes(listaMensajes):
    assert esListaStr(listaMensajes)
    if esListaVacia(listaMensajes):
        return
    else:
        print (cabeza(listaMensajes))
        return printMensajes(cola(listaMensajes))

# publicar: lista lista str str -> None/lista
# Verifica que el humano este en la red ingresada, luego agrega el 
# mensaje ingresado y el nombre del humano ingresado a un nodo, y este
# nodo lo agrega al timeline ingresado, retornando el timeline actualizado.
# Ej: publicar(tlD, redFinal, "Juan", "Chao") retorna el timeline
# lista(Nodo("Juan", "Chao"), lista(Nodo("Juan", "Sukistrukis"), lista(Nodo("Ben", "Como estan?"), lista(Nodo("Juan", "Hola"), listaVacia))))
# y publicar(tlD, redFinal, "Harry", "Hola") muestra en pantalla el mensaje Harry no pertenece a la red ingresada.
# Cuerpo de la funcion:
def publicar(timeline, unaRed, unHumano, mensaje):
    assert esLista(timeline) and esRedSocial(unaRed) and type(unHumano) == str and type(mensaje) == str
    if buscarHumano(unHumano, unaRed) == None:
        print (unHumano + " no pertenece a la red ingresada")
        return
    else:
        nodoMensaje = Nodo(unHumano, mensaje)
        timelineActualizado = lista(nodoMensaje, timeline)
        return timelineActualizado

# Para el siguiente ejemplo:
redx = agregarAmigos("Juan", "Ben", redFinal)

# visitar: lista lista str str -> None
# Recibe el timeline, la red social y el nombre de dos humanos.
# Luego verifica que los humanos esten en la red ingresada (muestra mensaje si no lo estan),
# luego y muestra en pantalla todos los mensajes publicados por el segundo humano ingresado,
# siempre y cuando ambos humanos sean amigos.
# Ej: visitar(tlD, redx, "Juan", "Ben") muestra en pantalla los siguientes mensajes:
# Juan visitando mensajes de Ben:
# Como estan?
# y visitar(tlD, redx, "Ben", "Juan") muestra en pantalla los siguientes mensajes:
# Ben visitando mensajes de Juan:
# Sukistrukis
# Hola
# Cuerpo de la funcion:
def visitar(timeline, unaRed, humanoA, humanoB):
    assert esLista(timeline) and esRedSocial(unaRed) and type(humanoA) == str and type(humanoB) == str
    if buscarHumano(humanoA, unaRed) == None and buscarHumano(humanoB, unaRed) == None:
        print(humanoA + " y " + humanoB + " no pertenecen a la red ingresada")
        return
    elif buscarHumano(humanoA, unaRed) == None:
        print(humanoA + " no pertenece a la red ingresada")
        return
    elif buscarHumano(humanoB, unaRed) == None:
        print(humanoB + " no pertenece a la red ingresada")
        return
    else:
        humanoUno = buscarHumano(humanoA, unaRed)
        humanoDos = buscarHumano(humanoB, unaRed)
        if buscarEnAmigos(humanoUno, humanoDos.Amigos) == False:
            print(humanoB + " no es amigo de " + humanoA + ", no puedes ver sus mensajes")
            return
        elif buscarEnAmigos(humanoDos, humanoUno.Amigos) == False:
            print(humanoA + " no es amigo de " + humanoB + ", no puedes ver sus mensajes")
            return
        else:
            listaNodos = buscarNodos(humanoB, timeline)
            if esListaVacia(listaNodos):
                print(humanoB + " no tiene mensajes en su timeline")
                return
            else:
                listaMensajes = mensajeNodos(listaNodos)
                print(humanoA + " visitando mensajes de " + humanoB + ":")
                printMensajes(listaMensajes)
                return
