package org.misproyectos.tusgastos.Controladores;

import jakarta.servlet.http.HttpServletRequest;
import org.misproyectos.tusgastos.Excepciones.UsuarioNoEncontradoException;
import org.misproyectos.tusgastos.Modelo.Usuario;
import org.misproyectos.tusgastos.Servicios.UsuarioServicio;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.mindrot.jbcrypt.BCrypt;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

@Controller
public class UsuarioControlador {
    @Autowired private UsuarioServicio servicio;

    @GetMapping("/home_usuarios")
    public String mostrarListaUsuarios(HttpServletRequest request) {
        return "home_usuarios";
    }

    @GetMapping("/usuarios")
    public String mostrarListaUsuarios(Model modelo) {
        List<Usuario> listaUsuarios = servicio.listarTodos();
        modelo.addAttribute("listaUsuarios", listaUsuarios);

        return "gestion_usuarios";
    }

    @GetMapping("/usuarios/nuevo")
    public String mostrarNuevoFormulario(Model modelo) {
        modelo.addAttribute("usuario", new Usuario());
        modelo.addAttribute("tituloPagina", "Agregar usuario");
        return "formulario_agregar_usuario";
    }

    @PostMapping("usuarios/agregar")
    public String agregarUsuario(Usuario usuario, RedirectAttributes ra) {
        try {
            String contrasena = usuario.getContrasena();
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] messageDigest = md.digest(contrasena.getBytes());
            StringBuilder hexString = new StringBuilder();
            for (byte b : messageDigest) {
                hexString.append(String.format("%02x", b));
            }
            usuario.setContrasena(hexString.toString());
            servicio.agregar(usuario);
            ra.addFlashAttribute("mensaje", "El usuario ha sido agregado exitosamente");
            return "redirect:/usuarios";
        }
        catch (Exception e) {
            // En caso de error, agregar mensaje de error al modelo
            ra.addFlashAttribute("mensaje", "Error al agregar el usuario");
            return "redirect:/usuarios";
        }
    }

    @GetMapping("usuarios/editar/{id}")
    public String mostrarFormularioEditar(@PathVariable("id") Integer id, Model modelo, RedirectAttributes ra) {
        try {
            Usuario usuario = servicio.buscar(id);
            modelo.addAttribute("usuario", usuario);
            return "formulario_editar_usuario";
        }
        catch (UsuarioNoEncontradoException e) {
            ra.addFlashAttribute("mensaje", "El usuario de ID " + id + " no pudo ser editado");
            e.printStackTrace();
            return "redirect:/usuarios";
        }
    }

    @PostMapping("/usuarios/editarUsuario/{id}")
    public String editarUsuario(@PathVariable("id") Integer id, @ModelAttribute("usuario") Usuario usuarioForm, RedirectAttributes ra) {
        try {
            // Retrieve the existing user from the database
            Usuario usuario = servicio.buscar(id);

            // Update the fields with the values from the form
            usuario.setNombre(usuarioForm.getNombre());
            usuario.setEmail(usuarioForm.getEmail());
            usuario.setEdad(usuarioForm.getEdad());
            usuario.setRut(usuarioForm.getRut());
            String contrasena = usuarioForm.getContrasena();

            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] messageDigest = md.digest(contrasena.getBytes());
            StringBuilder hexString = new StringBuilder();
            for (byte b : messageDigest) {
                hexString.append(String.format("%02x", b));
            }
            usuario.setContrasena(hexString.toString());

            // Save the updated user
            servicio.agregar(usuario);

            return "redirect:/usuarios";
        } catch (UsuarioNoEncontradoException e) {
            ra.addFlashAttribute("mensaje", "El usuario de ID " + id + " no pudo ser editado");
            e.printStackTrace();
            return "redirect:/usuarios";
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }


    @GetMapping("/usuarios/eliminar/{id}")
    public String eliminarUsuario(@PathVariable("id") Integer id, RedirectAttributes ra) {
        try {
            Usuario usuario = servicio.buscar(id);
            String nombreUsuario = usuario.getNombre();
            servicio.eliminar(id);
            ra.addFlashAttribute("mensaje", "El usuario " + nombreUsuario + " ha sido borrado.");
            return "redirect:/usuarios";
        } catch (UsuarioNoEncontradoException e) {
            ra.addFlashAttribute("mensaje", e.getMessage());
            return "redirect:/usuarios";
        }
    }

    public Usuario buscarUsuarioPorId(int id){
        try {
            System.out.println(id);
            return servicio.buscar(id);
        }
        catch (UsuarioNoEncontradoException e){
            System.out.println("usuario no encontrado");
            return null;
        }
    }
}
