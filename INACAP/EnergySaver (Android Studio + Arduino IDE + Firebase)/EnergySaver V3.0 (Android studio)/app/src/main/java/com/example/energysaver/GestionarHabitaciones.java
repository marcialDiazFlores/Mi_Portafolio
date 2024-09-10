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

public class GestionarHabitaciones extends AppCompatActivity {

    EditText gestion;
    int id;
    HabitacionesDB habitacionesDB;
    Button btnGuardar, btnEditar, btnEliminar, btnVolver;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_gestionar_habitaciones);

        gestion = findViewById(R.id.gestionHabitacion);
        btnGuardar = findViewById(R.id.btnGuardarHabitacion);
        btnEditar = findViewById(R.id.btnEditar);
        btnEliminar = findViewById(R.id.btnEliminar);
        btnVolver = findViewById(R.id.btnVolver);

        btnVolver.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent volver = new Intent(getApplicationContext(), MenuHabitaciones.class);
                startActivity(volver);
            }
        });

        btnEditar.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                guardarHabitacion();
            }
        });

        btnGuardar.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                guardarHabitacion();
            }
        });

        btnEliminar.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                borrar();
            }
        });

        Intent i = getIntent();
        Bundle bolsa = i.getExtras();
        id = bolsa.getInt("id");
        if (id != 0) {
            gestion.setText(bolsa.getString("nombre"));
            btnGuardar.setEnabled(false);
        } else {
            btnEditar.setEnabled(false);
            btnEliminar.setEnabled(false);
        }
    }

    private void limparCampos () {
        id = 0;
        gestion.setText("");
    }

    private Habitacion llenarDatosHab() {
        Habitacion habitacion = new Habitacion();
        String nombre = gestion.getText().toString();

        habitacion.setId(id);
        habitacion.setNombre(nombre);
        return habitacion;
    }

    private void guardarHabitacion() {
        habitacionesDB = new HabitacionesDB(getApplicationContext(), "HabitacionesDB.db", null, 1);
        Habitacion habitacion = llenarDatosHab();
        if ( id == 0) {
            habitacionesDB.agregar(habitacion);
            Toast.makeText(this, "Habitación guardada", Toast.LENGTH_SHORT).show();
        } else {
            habitacionesDB.actualizar(id, habitacion);
            btnEditar.setEnabled(false);
            btnEliminar.setEnabled(false);
            Toast.makeText(this, "Habitación actualizada", Toast.LENGTH_SHORT).show();
        }
    }

    private void borrar() {
        habitacionesDB = new HabitacionesDB(getApplicationContext(), "HabitacionesDB.db", null, 1);
        Habitacion habitacion = llenarDatosHab();
        if ( id == 0) {
            Toast.makeText(this, "No es posible eliminar habitación", Toast.LENGTH_SHORT).show();
        } else {
            habitacionesDB.eliminar(id);
            limparCampos();
            btnGuardar.setEnabled(true);
            btnEditar.setEnabled(false);
            btnEliminar.setEnabled(false);
            Toast.makeText(this, "Habitación eliminada", Toast.LENGTH_SHORT).show();
        }
    }
}