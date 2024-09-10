package org.misproyectos.tusgastos.Controladores;

import jakarta.servlet.http.HttpServletRequest;
import org.misproyectos.tusgastos.Excepciones.GastoNoEncontradoException;
import org.misproyectos.tusgastos.Modelo.Gasto;
import org.misproyectos.tusgastos.Servicios.GastoServicio;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import java.util.List;

@Controller
public class GastoControlador {
    @Autowired private GastoServicio servicio;
    @GetMapping("/gastos")
    public String mostrarListaGastos(HttpServletRequest request, Model modelo) {
        Integer usuarioId = (Integer) request.getSession().getAttribute("usuarioId");
        String usuarioNombre = (String) request.getSession().getAttribute("usuarioNombre");
        List<Gasto> listaGastos = servicio.listarTodos(usuarioId);
        modelo.addAttribute("usuarioNombre", usuarioNombre);
        modelo.addAttribute("listaGastos", listaGastos);

        return "gestion_gastos";
    }

    @GetMapping("/gastos/nuevo")
    public String mostrarNuevoFormulario(Model modelo) {
        modelo.addAttribute("gasto", new Gasto());
        modelo.addAttribute("tituloPagina", "Agregar gasto");
        return "formulario_agregar_gasto";
    }

    @PostMapping("gastos/agregar")
    public String agregarGasto(HttpServletRequest request, Gasto gasto, RedirectAttributes ra) {
        try {
            Integer usuarioId = (Integer) request.getSession().getAttribute("usuarioId");
            if (usuarioId != null) {
                System.out.println(usuarioId);
            }
            else {
                System.out.println("???");
            }
            gasto.setUsuarioId(usuarioId);
            servicio.agregar(gasto);
            ra.addFlashAttribute("mensaje", "El gasto ha sido agregado exitosamente");
            return "redirect:/gastos";
        }
        catch (Exception e) {
            // En caso de error, agregar mensaje de error al modelo
            ra.addFlashAttribute("mensaje", "Error al agregar el gasto");
            return "redirect:/gastos";
        }
    }

    @GetMapping("gastos/editar/{id}")
    public String mostrarFormularioEditar(@PathVariable("id") Integer id, Model modelo, RedirectAttributes ra) {
        try {
            Gasto gasto = servicio.buscar(id);
            modelo.addAttribute("gasto", gasto);
            return "formulario_editar_gasto";
        }
        catch (GastoNoEncontradoException e) {
            ra.addFlashAttribute("mensaje", "El gasto de ID " + id + " no pudo ser editado");
            e.printStackTrace();
            return "redirect:/gastos";
        }
    }

    @PostMapping("/gastos/editarGasto/{id}")
    public String editarGasto(@PathVariable("id") Integer id, @ModelAttribute("gasto") Gasto gastoForm, RedirectAttributes ra) {
        try {
            Gasto gasto = servicio.buscar(id);
            String descripcion = gasto.getDescripcion();

            gasto.setDescripcion(gastoForm.getDescripcion());
            gasto.setMonto(gastoForm.getMonto());
            gasto.setFecha(gastoForm.getFecha());
            gasto.setCategoria(gastoForm.getCategoria());

            servicio.agregar(gasto);
            ra.addFlashAttribute("mensaje", "El gasto '" + descripcion + "' fue editado exitosamente");

            return "redirect:/gastos";
        } catch (GastoNoEncontradoException e) {
            ra.addFlashAttribute("mensaje", "El gasto no pudo ser borrado.");
            e.printStackTrace();
            return "redirect:/gastos";
        }
    }


    @GetMapping("/gastos/eliminar/{id}")
    public String eliminarUsuario(@PathVariable("id") Integer id, RedirectAttributes ra) {
        try {
            Gasto gasto = servicio.buscar(id);
            String descripcion = gasto.getDescripcion();
            servicio.eliminar(id);
            ra.addFlashAttribute("mensaje", "El gasto '" + descripcion + "' ha sido borrado.");
            return "redirect:/gastos";
        } catch (GastoNoEncontradoException e) {
            ra.addFlashAttribute("mensaje", "El gasto no pudo ser borrado.");
            return "redirect:/gastos";
        }
    }
}
