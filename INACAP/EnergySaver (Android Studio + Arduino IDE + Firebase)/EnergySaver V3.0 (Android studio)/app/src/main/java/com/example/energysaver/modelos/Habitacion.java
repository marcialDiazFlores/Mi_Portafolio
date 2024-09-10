package com.example.energysaver.modelos;

public class Habitacion {

    //Atributos de la tabla habitaciones
    private int id;
    private String nombre;

    //Constructor de la clase - sin argumentos
    public Habitacion() {
    }

    //Constructor de los atributos
    public Habitacion(int id, String nombre) {
        this.id = id;
        this.nombre = nombre;
    }

    //Getter and Setter
    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getNombre() {
        return nombre;
    }

    public void setNombre(String nombre) {
        this.nombre = nombre;
    }

    @Override
    public String toString() {
        return "Habitacion{" +
                "id=" + id +
                ", nombre='" + nombre + '\'' +
                '}';
    }
}
