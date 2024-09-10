#!/usr/bin/env python
# -*- coding: utf-8 -*-
#Aviso: Cambie el genero de Hermione de "H" a "F"
#Importe el archivo de su tarea:
#from Tarea2 import *
from tarea2 import *
# Inicio del dialogo
print("Bienvenido(a) a TweetCat.\n")
red = crearRedSocial()
print("Agregando humanos a la red...")
red = unirHumano(humano("Harry", "M", 18, listaVacia), red)
red = unirHumano(humano("Ron", "M", 18, listaVacia), red)
red = unirHumano(humano("Hermione", "F", 18, listaVacia), red)
red = unirHumano(humano("Severus Snape", "M", 30, listaVacia), red)
red = unirHumano(humano("Albus Dumbledore", "M", 116, listaVacia), red)
red = unirHumano(humano("Ginny", "F", 16, listaVacia), red)
red = unirHumano(humano("Luna", "F", 17, listaVacia), red)
red = unirHumano(humano("Draco", "M", 18, listaVacia), red)
print("\nCreando amigos...")
red = agregarAmigos("Harry", "Ron", red)
red = agregarAmigos("Ron", "Hermione", red)
red = agregarAmigos("Hermione", "Harry", red)
red = agregarAmigos("Harry", "Ginny", red)
red = agregarAmigos("Harry", "Albus Dumbledore", red)
red = agregarAmigos("Luna", "Hermione", red)
red = agregarAmigos("Luna", "Ginny", red)
print("\nPublicando mensajes en el timeline...")
tl = listaVacia
tl = publicar(tl, red, "Harry", "Hola a todos!")
tl = publicar(tl, red, "Draco", "Los odio a todos >.>")
tl = publicar(tl, red, "Albus Dumbledore", "Hola! :D")
tl = publicar(tl, red, "Luna", "TweetCat es lo mejor! :3")
tl = publicar(tl, red, "Harry", "Saludos desde Hogwarts")
tl = publicar(tl, red, "Ginny", "Recién integrándome a TweetCat!")
print(" ")
mostrarHumanos(red)

print("\nMostrando algunos amigos:")
mostrarAmigos("Harry", red)
mostrarAmigos("Ron", red)
mostrarAmigos("Luna", red)
mostrarAmigos("Draco", red)
print("\nFiltrando...")
filtrar(red, 17, "F")
filtrar(red, 25, "M")
filtrar(red, 30, "F")

print("\nVisitando algunos muros:")
visitar(tl, red, "Ron", "Harry")
print(" ")
visitar(tl, red, "Hermione", "Luna")
print(" ")
visitar(tl, red, "Draco", "Harry")
print(" ")
visitar(tl, red, "Harry", "Ron")
print(" ")

