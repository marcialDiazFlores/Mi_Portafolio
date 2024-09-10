from Mapa import *
# Modulo de tests de la clase Mapa.
# Instancias de clase:
ju = JUtil()
ju2 = JUtil()
simple = Mapa(ju, "simple")
forest = Mapa(ju2, "forest")
# Tests del metodo libre:
assert simple.libre(3, 4)
assert not simple.libre(0, 1)
assert simple.libre(6, 10)
assert not forest.libre(0, 1)
assert forest.libre(3, 4)
assert not forest.libre(6, 10)