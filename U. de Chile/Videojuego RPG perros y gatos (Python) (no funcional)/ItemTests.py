from Item import *
# Modulo de tests de la clase Item.
# Instancias de clase:
ju = JUtil()
ju.crearVentana(24*32, 16*32)
mapa = Mapa(ju, "simple")
mapa.dibujar()
item1 = Item(ju, "Espada", 4, 4)
item2 = Item(ju, "Escudo", 5, 5)
item3 = Item(ju, "Pocion", 6, 7)
item4 = Item(ju, "Botas", 9, 10)
# Tests del metodo getNombre:
assert item1.getNombre() == "Espada"
assert item2.getNombre() == "Escudo"
assert item3.getNombre() == "Pocion"
assert item4.getNombre() == "Botas"
# Tests de los metodos getPosicion y destruir:
assert item1.getPosicion() == [4, 4]
assert item2.getPosicion() == [5, 5]
assert item3.getPosicion() == [6, 7]
assert item4.getPosicion() == [9, 10]
item1.destruir()
item2.destruir()
item3.destruir()
item4.destruir()
assert item1.getPosicion() == [100**100, 100**100]
assert item2.getPosicion() == [100**100, 100**100]
assert item3.getPosicion() == [100**100, 100**100]
assert item4.getPosicion() == [100**100, 100**100]