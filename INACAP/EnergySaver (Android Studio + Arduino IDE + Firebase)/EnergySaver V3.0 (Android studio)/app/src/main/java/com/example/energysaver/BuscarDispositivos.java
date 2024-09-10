package com.example.energysaver;

import com.google.firebase.database.DataSnapshot;
import com.google.firebase.database.DatabaseError;
import com.google.firebase.database.ValueEventListener;

import androidx.appcompat.app.AppCompatActivity;

import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.Toast;

//import com.example.energysaver.controladores.DispositivosDB;
import com.example.energysaver.modelos.Dispositivo;
import com.google.firebase.FirebaseApp;
import com.google.firebase.database.DatabaseReference;
import com.google.firebase.database.FirebaseDatabase;

public class BuscarDispositivos extends AppCompatActivity {

    Button btnBuscar, btnVolver;
    EditText editText;
    //DispositivosDB dispositivosDB;
    FirebaseDatabase firebaseDatabase;
    DatabaseReference databaseReference;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_buscar_dispositivos);

        btnBuscar = findViewById(R.id.btnBuscarDispo);
        btnVolver = findViewById(R.id.btnVolver);
        editText = findViewById(R.id.buscarDispo);

        btnVolver.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent volver = new Intent(getApplicationContext(), MenuDispositivos.class);
                startActivity(volver);
            }
        });

        iniciarFirebase();

        btnBuscar.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                if (view.getId() == R.id.btnBuscarDispo) {
                    String nombreDis = editText.getText().toString();
                    buscarDispositivoEnFirebase(nombreDis);
                }
            }

            private void buscarDispositivoEnFirebase(String nombreDis) {
                databaseReference.child("Dispositivo").orderByChild("nombre").equalTo(nombreDis)
                        .addListenerForSingleValueEvent(new ValueEventListener() {
                            @Override
                            public void onDataChange(DataSnapshot snapshot) {
                                if (snapshot.exists()) {
                                    // Dispositivo encontrado
                                    for (DataSnapshot dataSnapshot : snapshot.getChildren()) {
                                        Dispositivo dispositivo = dataSnapshot.getValue(Dispositivo.class);

                                        // Realiza la acción que necesitas con el dispositivo encontrado
                                        Bundle bolsa = new Bundle();
                                        //bolsa.putInt("idDis", dispositivo.getId());
                                        bolsa.putString("nombreDis", dispositivo.getNombre());

                                        Intent intent = new Intent(getApplicationContext(), GestionarDispositivos.class);
                                        intent.putExtras(bolsa);
                                        startActivity(intent);
                                    }
                                } else {
                                    // Dispositivo no encontrado
                                    Toast.makeText(BuscarDispositivos.this, "No existe dispositivo con ese nombre", Toast.LENGTH_SHORT).show();
                                }
                            }

                            @Override
                            public void onCancelled(DatabaseError error) {
                                // Maneja el error si es necesario
                            }
                        });
            }

        });
    }

    private void iniciarFirebase() {
        FirebaseApp.initializeApp(this);
        firebaseDatabase = FirebaseDatabase.getInstance();
        databaseReference = firebaseDatabase.getReference();
    }
}