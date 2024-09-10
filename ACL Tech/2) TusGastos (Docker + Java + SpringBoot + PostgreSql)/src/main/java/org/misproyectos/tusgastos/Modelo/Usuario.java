package org.misproyectos.tusgastos.Modelo;

import jakarta.persistence.*;

import java.util.List;

@Entity
@DiscriminatorValue("U")
public class Usuario extends Perfil {

    @Override
    public String mostrarDatos() {
        // Llama al método mostrarDatos() de la clase base (Perfil)
        String datosPerfil = super.mostrarDatos();

        // Agrega información específica de Usuario
        return datosPerfil + "\nHistorial de Gastos: " + "mostrarHistorial()";
    }

    public String mostrarDatos(String mensaje) {
        // Llama al método mostrarDatos() de la clase base (Perfil)
        String datosPerfil = super.mostrarDatos();

        // Agrega información específica de Usuario y un mensaje
        return datosPerfil + "\nHistorial de Gastos: " + "mostrarHistorial()" + mensaje;
    }
}
