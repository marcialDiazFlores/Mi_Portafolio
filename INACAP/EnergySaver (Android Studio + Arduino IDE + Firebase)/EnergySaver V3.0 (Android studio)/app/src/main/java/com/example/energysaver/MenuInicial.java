package com.example.energysaver;

import androidx.appcompat.app.AppCompatActivity;

import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.ImageButton;

public class MenuInicial extends AppCompatActivity {

    ImageButton btnConfig, btnHabitacion, btnDispositivo;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_menu_inicial);

        btnConfig = findViewById(R.id.btnConfiguracion);
        btnHabitacion = findViewById(R.id.btnHabitaciones);
        btnDispositivo = findViewById(R.id.btnDispositivos);

        btnConfig.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent config = new Intent(getApplicationContext(), MenuConfiguracion.class);
                startActivity(config);
                finish();
            }
        });

        btnHabitacion.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent room = new Intent(getApplicationContext(), MenuHabitaciones.class);
                startActivity(room);
                finish();
            }
        });

        btnDispositivo.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent dispo = new Intent(getApplicationContext(), MenuDispositivos.class);
                startActivity(dispo);
            }
        });
    }
}