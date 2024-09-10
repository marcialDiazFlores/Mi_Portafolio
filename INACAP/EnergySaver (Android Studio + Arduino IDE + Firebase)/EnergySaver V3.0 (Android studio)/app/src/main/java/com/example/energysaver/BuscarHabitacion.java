package com.example.energysaver;

import androidx.appcompat.app.AppCompatActivity;

import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.Toast;

import com.example.energysaver.controladores.HabitacionesDB;
import com.example.energysaver.modelos.Habitacion;

public class BuscarHabitacion extends AppCompatActivity {

    EditText buscarHabitacion;
    Button btnBuscarHabitacion, btnVolver;
    HabitacionesDB habitacionesDB;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_buscar_habitacion);

        buscarHabitacion = findViewById(R.id.buscarHabitacion);
        btnBuscarHabitacion = findViewById(R.id.btnBuscarHabitacion);
        btnVolver = findViewById(R.id.btnVolver);

        btnVolver.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent volver = new Intent(getApplicationContext(), MenuHabitaciones.class);
                startActivity(volver);
            }
        });

        btnBuscarHabitacion.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                if (view.getId() == R.id.btnBuscarHabitacion) {
                    String nombre = buscarHabitacion.getText().toString();
                    Habitacion habitacion = buscarHab(nombre);
                    if (nombre != null) {
                        Bundle bolsa = new Bundle();
                        bolsa.putInt("id", habitacion.getId());
                        bolsa.putString("nombre", habitacion.getNombre());

                        Intent intent = new Intent(getApplicationContext(), GestionarHabitaciones.class);
                        intent.putExtras(bolsa);
                        startActivity(intent);
                    } else {
                        Toast.makeText(BuscarHabitacion.this, "No existe la habitación con ese nombre", Toast.LENGTH_SHORT).show();
                    }
                }
            }

            private Habitacion buscarHab(String nombre) {
                habitacionesDB = new HabitacionesDB(getApplicationContext(), "HabitacionesDB.db", null, 1);
                Habitacion habitacion = habitacionesDB.elemento(nombre);
                return habitacion;
            }
        });
    }
}