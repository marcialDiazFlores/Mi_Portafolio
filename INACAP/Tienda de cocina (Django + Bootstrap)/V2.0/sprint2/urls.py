from django.urls import path
from . import views

urlpatterns = [
    path('', views.index, name=''),  # Ruta para la vista de inicio
    path('index/', views.index, name='index'),  # Ruta para la vista de inicio

    #Formularios
    path("formcontacto/", views.formContacto, name="formcontacto"),
    path('formcontacto/gracias/', views.gracias, name='gracias'),

    #Busqueda por Categorias
    path('buscar/', views.buscar_productos, name='buscar'),
    path('prodsxcateg/', views.productos_categoria, name="productos-categoria"),
    path('categoria/<int:categoria_id>/', views.productos_por_categoria, name='productos-por-categoria'),

    #Vista de Listas
    path('prodsxcateg', views.productos_categoria, name='productos-categoria'),
    path('productos/', views.ProductoListView.as_view(), name="productos"),
    path('cuentas/', views.CuentaListView.as_view(), name="cuentas"),
    path('mensajesFormulario/', views.MensajeContactoListView.as_view(), name="mensajesFormulario"),
    path('categorias/', views.CategoriaListView.as_view(), name="categorias"),

    # Detalles de Productos
    path('productos/<int:pk>', views.ProductoDetailView.as_view(), name="producto-detail"),
    path('cuentas/<int:pk>', views.CuentaDetailView.as_view(), name="cuenta-detail"),
    path('categorias/<int:pk>', views.CategoriaDetailView.as_view(), name="categoria-detail"),
]