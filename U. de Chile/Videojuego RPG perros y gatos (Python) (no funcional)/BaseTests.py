from Base import *
# Modulo de tests de la clase Base.
# Instancias de clase:
ju = JUtil()
ju.crearVentana(24*32, 16*32)
mapa = Mapa(ju, "simple")
mapa.dibujar()
base1 = Base(ju, "gato")
base2 = Base(ju, "perro")
# Tests del metodo getPosicion:
assert base1.getPosicion() == [3, 4]
assert base2.getPosicion() == [21, 12]
# Tests del metodo recibeAtaque:
assert base1.recibeAtaque(40)
assert base2.recibeAtaque(60)
assert not base1.recibeAtaque(60)
assert not base2.recibeAtaque(100)
# Tests del metodo salida:
m = base1.salida()
n = base2.salida()
assert m == [4, 4] or m == [2, 4] or m == [3, 3] or m == [3, 5]
assert n == [22, 12] or n == [20, 12] or n == [21, 11] or n == [21, 13]