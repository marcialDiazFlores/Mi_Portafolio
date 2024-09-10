package com.example.energysaver;

import androidx.appcompat.app.AppCompatActivity;

import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.widget.AdapterView;
import android.widget.ArrayAdapter;
import android.widget.Button;
import android.widget.ListView;

import com.example.energysaver.controladores.HabitacionesDB;
import com.example.energysaver.controladores.SelectListener;
import com.example.energysaver.modelos.Habitacion;

import java.util.ArrayList;
import java.util.List;

public class ListadoHabitaciones extends AppCompatActivity implements SelectListener {

    ListView listView;
    ArrayList<String> nombreHabitacion;
    ArrayList<Integer> idHabitacion;
    HabitacionesDB habitacionesDB;
    Button btnVolver;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_listado_habitaciones);

        habitacionesDB = new HabitacionesDB(getApplicationContext(), "HabitacionesDB.db", null, 1);
        listView = findViewById(R.id.listHabitaciones);
        btnVolver = findViewById(R.id.btnVolver);

        btnVolver.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent volver = new Intent(getApplicationContext(), MenuHabitaciones.class);
                startActivity(volver);
            }
        });

        llenarLista();
    }

    private void llenarLista() {
        nombreHabitacion = new ArrayList<String>();
        idHabitacion = new ArrayList<Integer>();

        List<Habitacion> habitacionList = habitacionesDB.lista();
        for (int i = 0; i<habitacionList.size(); i++) {
            Habitacion habitacion = habitacionList.get(i);
            nombreHabitacion.add(habitacion.getNombre());
            idHabitacion.add(habitacion.getId());
        }

        ArrayAdapter<String> adapter = new ArrayAdapter<String>(
                getApplicationContext(),
                android.R.layout.simple_list_item_1,
                nombreHabitacion);
        listView.setAdapter(adapter);
        listView.setOnItemClickListener(new AdapterView.OnItemClickListener() {
            @Override
            public void onItemClick(AdapterView<?> adapterView, View view, int i, long id) {
                Habitacion habitacion = habitacionesDB.elemento(idHabitacion.get(i));
                Bundle bolsa = new Bundle();
                bolsa.putInt("id", habitacion.getId());
                bolsa.putString("nombre", habitacion.getNombre());

                Intent intent = new Intent(getApplicationContext(), GestionarHabitaciones.class);
                intent.putExtras(bolsa);
                startActivity(intent);
            }
        });
    }

    @Override
    public void onItemClick(String nombre) {
    }
}