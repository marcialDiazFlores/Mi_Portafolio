package com.example.energysaver.controladores;

import com.example.energysaver.modelos.Habitacion;

import java.util.List;

public interface IHabitacionesDB {

    Habitacion elemento(int id);
    Habitacion elemento(String nombre);

    List<Habitacion> lista();

    void agregar (Habitacion room);
    void actualizar (int id, Habitacion room);
    void eliminar (int id);
}