package com.example.energysaver;

import androidx.appcompat.app.AppCompatActivity;

import android.content.Context;
import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.Toast;

public class MenuHabitaciones extends AppCompatActivity {

    Button btnAdd, btnSearch, btnList, btnVolver;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_menu_habitaciones);

        btnAdd = findViewById(R.id.btnAddRoom);
        btnSearch = findViewById(R.id.btnSearchRoom);
        btnList = findViewById(R.id.btnListRoom);
        btnVolver = findViewById(R.id.btnVolver);

        btnSearch.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent buscar = new Intent(getApplicationContext(), BuscarHabitacion.class);
                startActivity(buscar);
            }
        });

        btnAdd.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent add = new Intent(getApplicationContext(), GestionarHabitaciones.class);
                Bundle bolsa = new Bundle();
                bolsa.putInt("id", 0);
                add.putExtras(bolsa);
                startActivity(add);
            }
        });

        btnList.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent lista = new Intent(getApplicationContext(), ListadoHabitaciones.class);
                startActivity(lista);
            }
        });

        btnVolver.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent volver = new Intent(getApplicationContext(), MenuInicial.class);
                startActivity(volver);
            }
        });

    }
}