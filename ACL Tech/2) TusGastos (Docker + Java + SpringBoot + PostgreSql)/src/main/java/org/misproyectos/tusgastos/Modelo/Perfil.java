package org.misproyectos.tusgastos.Modelo;

import jakarta.persistence.*;

@Entity
@Table(name = "perfiles", schema = "public")
@Inheritance(strategy = InheritanceType.SINGLE_TABLE)
@DiscriminatorColumn(name = "DTYPE", discriminatorType = DiscriminatorType.STRING)
public class Perfil {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;

    @Column(nullable = false, length = 80, name = "nombre")
    private String nombre;
    @Column(nullable = false, unique = false, length = 45, name = "email")
    private String email;
    @Column(nullable = false, length = 80, name = "contraseña")
    private String contrasena;
    @Column(nullable = false, name = "edad")
    private Integer edad;
    @Column(nullable = false, unique = false, length = 14, name = "rut")
    private String rut;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getNombre() {
        return nombre;
    }

    public void setNombre(String nombre) {
        this.nombre = nombre;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getContrasena() {
        return contrasena;
    }

    public void setContrasena(String contrasena) {
        this.contrasena = contrasena;
    }

    public Integer getEdad() {
        return edad;
    }

    public void setEdad(Integer edad) {
        this.edad = edad;
    }

    public String getRut() {
        return rut;
    }

    public void setRut(String rut) {
        this.rut = rut;
    }

    public String mostrarDatos() {
        return "ID: " + id +
                "\nNombre: " + nombre +
                "\nEmail: " + email +
                "\nContraseña: " + contrasena +
                "\nEdad: " + edad +
                "\nRUT: " + rut;
    }
}
