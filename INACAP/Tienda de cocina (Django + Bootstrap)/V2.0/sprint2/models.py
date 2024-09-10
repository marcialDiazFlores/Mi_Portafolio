from django.db import models
from django.urls import reverse

# Create your models here.

class Profesion(models.Model):
    profesion = models.CharField(max_length=100, help_text="Ingresa tu profesión")

    class Meta:
        verbose_name = "Profesión"
        verbose_name_plural = "Profesiones"

    def __str__(self):
        return f'{self.profesion}'

class Cuenta(models.Model):
    nombre = models.CharField(max_length = 100, help_text = "Ingresa tu nombre")
    apellido = models.CharField(max_length = 100, help_text = "Ingresa tu apellido")
    fecha_nac = models.DateField(null = True, help_text = "Indica tu fecha de nacimiento")
    correo = models.EmailField(null = True, blank = True, help_text = "Correo personal")
    profesion = models.ManyToManyField(Profesion, help_text="Ingresa tu profesión")

    def __str__(self):
        return f'{self.nombre} {self.apellido}'

    def get_absolute_url(self):
        return reverse("cuenta_detail", kwargs={"pk": self.id})
    
    def display_profesion(self):
        #Se crea un String con las profesiones
        return ', '.join([prof.profesion for prof in self.profesion.all()[:3]])
    
    display_profesion.short_description = 'profesion'

class Producto (models.Model):

    nombre = models.CharField(max_length=100, help_text="Ingresa nombre Producto")
    stock = models.IntegerField(help_text="Stock de Producto")
    precio = models.IntegerField(null=True, help_text="Precio de Producto")
    imagen = models.ImageField(upload_to='productos', null=True)
    categoria = models.ForeignKey('Categoria', on_delete=models.SET_NULL, null=True)
    descripcion = models.CharField(max_length=200, help_text="Ingresa descripción del producto", null = True)
         
    def __str__(self):
        return f'{self.nombre}'
    
    def get_absolute_url(self):
        return reverse("producto-detail", args=[str(self.id)])

class Categoria (models.Model):
    nombre = models.CharField(max_length=100, help_text="Ingresa nombre Categoría")
   
    def __str__(self):
        return f'{self.nombre}'

    def get_absolute_url(self):
        return reverse("productos-por-categoria", kwargs={"pk": self.id})
    
class MensajeContacto(models.Model):
    nombre = models.CharField(max_length=100)
    correo = models.EmailField(max_length=100)
    fono = models.CharField(max_length=100, blank=True, null=True)
    comentario = models.TextField()
    cc_ami = models.BooleanField(default=False)

    class Meta:
        verbose_name = "Mensaje de contacto"
        verbose_name_plural = "Mensajes de contacto"

    def __str__(self):
        return f'{self.nombre}' 

    def get_absolute_url(self):
        return reverse("mensajesFormulario-detail", kwargs={"pk": self.id})  