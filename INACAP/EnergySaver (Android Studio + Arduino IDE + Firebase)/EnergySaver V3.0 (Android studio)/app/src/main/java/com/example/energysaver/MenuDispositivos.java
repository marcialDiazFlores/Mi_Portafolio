package com.example.energysaver;

import androidx.appcompat.app.AppCompatActivity;

import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;

public class MenuDispositivos extends AppCompatActivity {

    Button btnBuscar, btnRegistrar, btnLista, btnVolver;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_menu_dispositivos);

        btnBuscar = findViewById(R.id.btnSearchDispo);
        btnRegistrar = findViewById(R.id.btnAddDispo);
        btnLista = findViewById(R.id.btnListDispo);
        btnVolver = findViewById(R.id.btnVolver);

        btnVolver.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent volver = new Intent(getApplicationContext(), MenuInicial.class);
                startActivity(volver);
            }
        });

        btnRegistrar.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent gestion = new Intent(getApplicationContext(), GestionarDispositivos.class);
                Bundle bolsa = new Bundle();
                bolsa.putInt("idDis", 0);
                gestion.putExtras(bolsa);
                startActivity(gestion);
            }
        });

        btnBuscar.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent buscar = new Intent(getApplicationContext(), BuscarDispositivos.class);
                startActivity(buscar);
            }
        });

        btnLista.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent lista = new Intent(getApplicationContext(), ListadoDispositivos.class);
                startActivity(lista);
            }
        });
    }
}