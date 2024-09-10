package org.misproyectos.tusgastos.Controladores;

import org.mindrot.jbcrypt.BCrypt;
import org.misproyectos.tusgastos.Excepciones.UsuarioNoEncontradoException;
import org.misproyectos.tusgastos.Modelo.Administrador;
import org.misproyectos.tusgastos.Modelo.Perfil;
import org.misproyectos.tusgastos.Modelo.Usuario;
import org.misproyectos.tusgastos.Servicios.AdministradorServicio;
import org.misproyectos.tusgastos.Servicios.UsuarioServicio;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import java.util.List;

@Controller
public class AdministradorControlador {
    @Autowired private AdministradorServicio servicio;

    @GetMapping("/home_admin")
    public String mostrarFormularioLogin() {
        return "home_admin";
    }

    @GetMapping("/admin/nuevo")
    public String mostrarNuevoFormulario(Model modelo) {
        modelo.addAttribute("administrador", new Administrador());
        return "formulario_agregar_admin";
    }

    @PostMapping("/admin/agregar")
    public String agregarAdministrador(Administrador admin, RedirectAttributes ra) {
        try {
            String contrasena = admin.getContrasena();
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] messageDigest = md.digest(contrasena.getBytes());
            StringBuilder hexString = new StringBuilder();
            for (byte b : messageDigest) {
                hexString.append(String.format("%02x", b));
            }
            admin.setContrasena(hexString.toString());
            servicio.agregar(admin);
            ra.addFlashAttribute("mensaje", "El administrador ha sido agregado exitosamente");
            return "redirect:/login";
        }
        catch (Exception e) {
            // En caso de error, agregar mensaje de error al modelo
            ra.addFlashAttribute("mensaje", "Error al agregar al administrador");
            return "redirect:/login";
        }
    }
}
