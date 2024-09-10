package org.misproyectos.tusgastos.Repositorios;

import org.misproyectos.tusgastos.Modelo.Usuario;
import org.springframework.data.repository.CrudRepository;

public interface UsuarioRepositorio extends CrudRepository<Usuario, Integer> {
    public Long countById(Integer id);
}
