package org.misproyectos.tusgastos.Servicios;

import org.misproyectos.tusgastos.Excepciones.UsuarioNoEncontradoException;
import org.misproyectos.tusgastos.Modelo.Perfil;
import org.misproyectos.tusgastos.Modelo.Usuario;
import org.misproyectos.tusgastos.Repositorios.PerfilRepositorio;
import org.misproyectos.tusgastos.Repositorios.UsuarioRepositorio;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
public class PerfilServicio {
    @Autowired private PerfilRepositorio repositorioPerfil;

    public Perfil obtenerPerfilPorEmailYContrasena(String email, String contrasena) {
        System.out.println(email);
        System.out.println(contrasena);
        return repositorioPerfil.obtenerPerfilPorEmailYContrasena(email, contrasena);
    }
}
