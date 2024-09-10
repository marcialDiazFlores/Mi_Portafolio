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
public class UsuarioServicio {
    @Autowired private UsuarioRepositorio repositorio;

    public List<Usuario> listarTodos() {
        return (List<Usuario>) repositorio.findAll();
    }

    public void agregar(Usuario usuario) {
        repositorio.save(usuario);
    }

    public Usuario buscar(Integer id) throws UsuarioNoEncontradoException {
        Optional<Usuario> resultado = repositorio.findById(id);
        if (resultado.isPresent()) {
            System.out.println("encontrado");
            return resultado.get();
        }
        throw new UsuarioNoEncontradoException("No se encontraron usuarios con el ID " + id);
    }

    public void eliminar(Integer id) throws UsuarioNoEncontradoException {
        Long buscar = repositorio.countById(id);
        if (buscar == null || buscar == 0) {
            throw new UsuarioNoEncontradoException("No se encontraron usuarios con el ID " + id);
        }
        repositorio.deleteById(id);
    }
}
