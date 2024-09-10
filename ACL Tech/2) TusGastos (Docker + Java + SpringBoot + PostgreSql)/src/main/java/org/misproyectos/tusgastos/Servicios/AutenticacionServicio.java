package org.misproyectos.tusgastos.Servicios;

import org.hibernate.boot.model.internal.PersistentAttributeFilter;
import org.misproyectos.tusgastos.Modelo.Perfil;
import org.misproyectos.tusgastos.Servicios.UsuarioServicio;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class AutenticacionServicio {

    @Autowired
    private PerfilServicio perfilServicio;

    public Perfil autenticarPerfil(String email, String contrasena) {
        Perfil perfil = perfilServicio.obtenerPerfilPorEmailYContrasena(email, contrasena);

        return perfil;
    }
}
