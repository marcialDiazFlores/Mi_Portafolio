package org.misproyectos.tusgastos.Controladores;

import jakarta.servlet.http.HttpSession;
import org.misproyectos.tusgastos.Modelo.Administrador;
import org.misproyectos.tusgastos.Modelo.Perfil;
import org.misproyectos.tusgastos.Modelo.Usuario;
import org.misproyectos.tusgastos.Servicios.AutenticacionServicio;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.mindrot.jbcrypt.BCrypt;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

@Controller
public class LoginControlador {

    @Autowired
    private AutenticacionServicio autenticacionServicio;

    @GetMapping("/")
    public String mostrarPaginaLogin(Model modelo) {
        modelo.addAttribute("perfil", new Perfil());
        return "login";
    }

    @GetMapping("/login")
    public String mostrarFormularioLogin(Model modelo) {
        modelo.addAttribute("perfil", new Perfil());
        return "login";
    }

    @PostMapping("/login")
    public String procesarLogin(@RequestParam String email, @RequestParam String contrasena, Model modelo, RedirectAttributes ra, HttpSession session) throws NoSuchAlgorithmException {

        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] messageDigest = md.digest(contrasena.getBytes());
        StringBuilder hexString = new StringBuilder();
        for (byte b : messageDigest) {
            hexString.append(String.format("%02x", b));
        }
        String hash = hexString.toString();
        Perfil perfil = autenticacionServicio.autenticarPerfil(email, hash);

        if (perfil != null) {
            if (perfil instanceof Usuario) {
                session.setAttribute("usuarioId", perfil.getId());
                session.setAttribute("usuarioNombre", perfil.getNombre());
                modelo.addAttribute("perfil", perfil);
                // Redirigir a la página de usuario
                return "redirect:/gastos";
            } else if (perfil instanceof Administrador) {
                session.setAttribute("adminId", perfil.getId());
                session.setAttribute("adminNombre", perfil.getNombre());
                modelo.addAttribute("perfil", perfil);
                // Redirigir a la página de administrador
                return "redirect:/home_admin";
            }
        }

        // En caso de credenciales incorrectas, mostrar mensaje de error
        modelo.addAttribute("error", "Credenciales incorrectas");
        ra.addFlashAttribute("error", "Credenciales incorrectas");
        return "redirect:/login";
    }
}
