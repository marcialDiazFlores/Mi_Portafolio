from django.contrib import admin
from sprint2.models import Producto, Cuenta, Categoria, MensajeContacto, Profesion

# Register your models here.

#Se utiliza clase en la definición de modelos relacionados.
#class ProductoInLine (admin.StackedInLine)
class ProductoInLine(admin.TabularInline):
    model = Producto
    #Extra permite definit los arreglos adicionales vacios en el admin.
    extra= 0

class CuentaAdmin(admin.ModelAdmin):
    list_display = ('id', 'nombre', 'apellido', 'fecha_nac', 'correo', 'display_profesion')
    list_filter = ('apellido', 'fecha_nac')
    fieldsets = (
        ('Información Personal', {
            'fields': ('nombre', 'apellido',)
        }),
        ('Información Adicional', {
            'fields': ('fecha_nac', 'correo', 'profesion',)
        })
    )

class ProfesionAdmin(admin.ModelAdmin):
    list_display = ('id','profesion',)
    list_filter = ('profesion',)

class MensajeContactoAdmin(admin.ModelAdmin):
    list_display = ('id', 'nombre', 'correo', 'fono', 'comentario', 'cc_ami',)
    list_filter = ('id', 'correo')

class ProductoAdmin(admin.ModelAdmin):
    list_display = ('id', 'nombre', 'stock', 'precio',)
    list_filter = ('nombre', 'id')

class CategoriaAdmin(admin.ModelAdmin):
    list_display = ('id', 'nombre',)
    list_filter = ('nombre',)
    inlines = [ProductoInLine]

admin.site.register(Producto, ProductoAdmin)
admin.site.register(Cuenta, CuentaAdmin)
admin.site.register(Categoria, CategoriaAdmin)
admin.site.register(MensajeContacto, MensajeContactoAdmin)
admin.site.register(Profesion, ProfesionAdmin)