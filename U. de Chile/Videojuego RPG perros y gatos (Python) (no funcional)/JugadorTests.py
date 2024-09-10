from Jugador import *
# Modulo de tests de la clase Jugador.
# Instancias de clase:
ju = JUtil()
ju.crearVentana(24*32, 16*32)
mapa = Mapa(ju, "simple")
mapa.dibujar()
jugador1 = Jugador(ju, "gato")
jugador2 = Jugador(ju, "perro")
# Tests de los metodo mover y getPosicion:
jugador1.mover(4, 5)
assert jugador1.getPosicion() == [4, 5]
jugador1.mover(16, 3)
assert jugador1.getPosicion() == [16, 3]
jugador2.mover(8, 8)
assert jugador2.getPosicion() == [8, 8]
jugador2.mover(9, 10)
assert jugador2.getPosicion() == [9, 10]
# Tests de los metodo recibeAtaque y getVida:
assert jugador1.recibeAtaque(50)
assert jugador1.getVida() == 80
assert not jugador1.recibeAtaque(80)
assert jugador1.getVida() == 0
assert jugador2.recibeAtaque(90)
assert jugador2.getVida() == 10
assert not jugador2.recibeAtaque(100)
assert jugador2.getVida() == 0
# Tests de los metodo mejora, getVida, getAtaque, getDefensa y getVelocidad:
assert jugador1.getAtaque() == 32
jugador1.mejora("A", 34)
assert jugador1.getAtaque() == 66
assert jugador1.getDefensa() == 28
jugador1.mejora("D", 0)
assert jugador1.getDefensa() == 28
assert jugador2.getVelocidad() == 6
jugador2.mejora("V", 23)
assert jugador2.getVelocidad() == 29
assert jugador2.getVida() == 0
jugador2.mejora("H", 90)
assert jugador2.getVida() == 90
