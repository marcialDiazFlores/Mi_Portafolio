package org.misproyectos.tusgastos.Modelo;
import jakarta.persistence.*;

@Entity
@DiscriminatorValue("A") // "A" se asocia con Administrador
public class Administrador extends Perfil {
    @Override
    public String mostrarDatos() {
        // Llama al método mostrarDatos() de la clase base (Perfil)
        String datosPerfil = super.mostrarDatos();

        // Agrega información específica de Usuario
        return datosPerfil + "soy un administrador";
    }
}
