package com.example.energysaver.controladores;

import com.example.energysaver.modelos.Dispositivo;

import java.util.List;

public interface IDispositivosDB {

    Dispositivo elemento (int id);
    Dispositivo elemento (String nombre);

    List<Dispositivo> listaDispositivo();

    void agregarDis (Dispositivo device);
    void actualizarDis (int idDispositivo, Dispositivo device);
    void eliminarDis (int idDispositivo);
}
