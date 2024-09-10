package org.misproyectos.tusgastos.Servicios;

import org.misproyectos.tusgastos.Excepciones.UsuarioNoEncontradoException;
import org.misproyectos.tusgastos.Modelo.Administrador;
import org.misproyectos.tusgastos.Modelo.Perfil;
import org.misproyectos.tusgastos.Modelo.Usuario;
import org.misproyectos.tusgastos.Repositorios.AdministradorRepositorio;
import org.misproyectos.tusgastos.Repositorios.PerfilRepositorio;
import org.misproyectos.tusgastos.Repositorios.UsuarioRepositorio;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
public class AdministradorServicio {
    @Autowired private AdministradorRepositorio repositorio;
    @Autowired private PerfilRepositorio repositorioPerfil;

    public List<Administrador> listarTodos() {
        return (List<Administrador>) repositorio.findAll();
    }

    public void agregar(Administrador administrador) {
        repositorio.save(administrador);
    }

    public Administrador buscar(Integer id) throws UsuarioNoEncontradoException {
        Optional<Administrador> resultado = repositorio.findById(id);
        if (resultado.isPresent()) {
            return resultado.get();
        }
        throw new UsuarioNoEncontradoException("No se encontraron administradores con el ID " + id);
    }

    public void eliminar(Integer id) throws UsuarioNoEncontradoException {
        Long buscar = repositorio.countById(id);
        if (buscar == null || buscar == 0) {
            throw new UsuarioNoEncontradoException("No se encontraron usuarios con el ID " + id);
        }
        repositorio.deleteById(id);
    }

    public Perfil obtenerPerfilPorEmailYContrasena(String email, String contrasena) {
        return repositorioPerfil.obtenerPerfilPorEmailYContrasena(email, contrasena);
    }
}
