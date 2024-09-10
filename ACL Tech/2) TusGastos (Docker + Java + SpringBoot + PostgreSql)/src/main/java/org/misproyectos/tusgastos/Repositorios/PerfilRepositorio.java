package org.misproyectos.tusgastos.Repositorios;

import org.misproyectos.tusgastos.Modelo.Perfil;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;

public interface PerfilRepositorio extends CrudRepository<Perfil, Integer> {
    public Integer countById(Integer id);

    @Query("SELECT p FROM Perfil p WHERE p.email = :email AND p.contrasena = :contrasena")
    Perfil obtenerPerfilPorEmailYContrasena(@Param("email") String email, @Param("contrasena") String contrasena);
}
