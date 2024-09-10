from django.shortcuts import render
from .models import Cuenta, Producto, Categoria, MensajeContacto
from django.views import generic
from .forms import FormularioContacto
from django.http import HttpResponseRedirect
from django.core.mail import send_mail
from django.core.paginator import Paginator
from django.shortcuts import render, get_object_or_404

# Create your views here.
def index(request):
    cant_cuentas = Cuenta.objects.all().count()
    cant_productos = Producto.objects.all().count()
    cant_categorias = Categoria.objects.all().count()
    contexto = {'cant_cuentas':cant_cuentas,
                'cant_productos': cant_productos,
                'cant_categorias': cant_categorias}
    return render(request, 'sprint2/index.html')

def buscar_productos(request):
    categorias = Categoria.objects.all()
    return render(request, "sprint2/buscar_productos.html", {"categorias": categorias})

def productos_categoria(request):
    if (request.GET["cod_categoria"] and (request.GET["cod_categoria"]) != "0"):
        cod_categoria = request.GET["cod_categoria"]
        lst_productos = Producto.objects.filter(categoria__exact=cod_categoria)
        categoria = Categoria.objects.get(id__exact=cod_categoria)
        contexto = {"productos": lst_productos, "categoria": categoria}

    else:
        contexto = {"categoria": "Debe seleccionar una categoría"}

    return render(request, "sprint2/productos_categoria.html", contexto)

def formContacto(request):
    if request.method == "POST":
        "Se crea una instancia del formulario (clase de forms.py)"
        form = FormularioContacto(request.POST)
        if form.is_valid():
            nombref = form.cleaned_data['nombre']
            correof = form.cleaned_data['correo']
            fonof = form.cleaned_data['fono']
            comentariof = form.cleaned_data['comentario']
            cc_amif = form.cleaned_data['cc_ami']
            contacto = MensajeContacto(nombre = nombref, correo = correof, fono = fonof, comentario = comentariof, cc_ami = cc_amif)
            contacto.save()
            #Se obtiene información del formulario
            #form.save() #Se guarda la informaición de contacto en el Admin
            destinatarios = ['kitchen.supplies.contacto@gmail.com']
            if cc_amif:
                destinatarios.append(correof)
            send_mail("Copia envío formulario", comentariof, "Kitchen Supplies (Contacto)", destinatarios)
            return HttpResponseRedirect("gracias/")
    else:
        form = FormularioContacto()
    return render(request, "sprint2/formcontacto.html", {"form": form})

def gracias(request):
    return render(request, "sprint2/gracias.html")

class ProductoListView(generic.ListView):
    model = Producto
    paginate_by = 6

class CuentaListView(generic.ListView):
    model = Cuenta

class CategoriaListView(generic.ListView):
    model = Categoria

class MensajeContactoListView(generic.ListView):
    model = MensajeContacto
    paginate_by = 6
    
# Creación de vistas de detalle, basada en Clases.
class ProductoDetailView(generic.DetailView):
    model = Producto

class CuentaDetailView(generic.DetailView):
    model = Cuenta

class CategoriaDetailView(generic.DetailView):
    model = Categoria

def productos_por_categoria(request, categoria_id):
    categoria = get_object_or_404(Categoria, pk=categoria_id)
    productos = Producto.objects.filter(categoria=categoria)
    return render(request, 'sprint2/productos_category.html', {'categoria': categoria, 'productos': productos})