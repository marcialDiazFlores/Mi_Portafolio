package com.example.energysaver.modelos;

public class Dispositivo {

    //private static int contadorId = 0;
    //private int id;
    private String nombre;
    private Boolean encendido;
    private Boolean movimiento;

    public Dispositivo() {
    }

    public Dispositivo(/*int id, */String nombre) {
        //this.id = generarId();
        this.nombre = nombre;
        this.encendido = false;
        this.movimiento = false;
    }

    /*
    private int generarId() {
        return contadorId++;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }
    */

    public String getNombre() {
        return nombre;
    }

    public void setNombre(String nombre) {
        this.nombre = nombre;
    }

    public boolean isEncendido() {
        return encendido;
    }

    public void setEncendido(boolean encendido) {
        this.encendido = encendido;
    }

    public boolean isMovimiento() {
        return movimiento != null && movimiento;
    }

    public void setMovimiento(boolean movimiento) {
        this.movimiento = movimiento;
    }

    @Override
    public String toString() {
        return "Dispositivo{" +
                ", nombre='" + nombre + '\'' +
                "encendido: " + encendido + '}';
    }
}