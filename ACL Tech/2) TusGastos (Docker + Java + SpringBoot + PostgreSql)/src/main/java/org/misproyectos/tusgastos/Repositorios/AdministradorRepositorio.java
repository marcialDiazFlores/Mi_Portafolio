package org.misproyectos.tusgastos.Repositorios;

import org.misproyectos.tusgastos.Modelo.Administrador;
import org.springframework.data.repository.CrudRepository;

public interface AdministradorRepositorio extends CrudRepository<Administrador, Integer> {
    public Long countById(Integer id);
}
