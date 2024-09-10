package org.misproyectos.tusgastos.Repositorios;

import org.misproyectos.tusgastos.Modelo.Gasto;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface GastoRepositorio extends CrudRepository<Gasto, Integer> {
    public Integer countById(Integer id);

    @Query("SELECT g FROM Gasto g WHERE g.usuarioId = :id ORDER BY g.fecha ASC")
    List<Gasto> findAllOrderByFechaAsc(@Param("id") Integer id);
}
