package com.example.energysaver;

import androidx.annotation.NonNull;
import androidx.appcompat.app.AppCompatActivity;

import com.example.energysaver.R;


import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.widget.AdapterView;
import android.widget.ArrayAdapter;
import android.widget.Button;
import android.widget.ListView;

//import com.example.energysaver.controladores.DispositivosDB;
import com.example.energysaver.controladores.SelectListenerDispo;
import com.example.energysaver.modelos.Dispositivo;
import com.example.energysaver.adaptadores.ListViewDispositivosAdapter;

import com.google.firebase.FirebaseApp;
import com.google.firebase.database.DataSnapshot;
import com.google.firebase.database.DatabaseError;
import com.google.firebase.database.DatabaseReference;
import com.google.firebase.database.FirebaseDatabase;
import com.google.firebase.database.ValueEventListener;

import java.util.ArrayList;
import java.util.List;

public class ListadoDispositivos extends AppCompatActivity implements SelectListenerDispo {

    Button btnVolver;
    ListView listViewDispo;
    ListViewDispositivosAdapter dispositivoAdapter;
    ArrayList<String> nombreDispositivo;
    ArrayList<Integer> idDispositivo;
    //DispositivosDB dispositivosDB;
    FirebaseDatabase firebaseDatabase;
    DatabaseReference databaseReference;
    private List<Dispositivo> listDispositivo = new ArrayList<Dispositivo>();

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_listado_dispositivos);

        btnVolver = findViewById(R.id.btnVolver);
        listViewDispo = findViewById(R.id.listDispo);
        dispositivoAdapter = new ListViewDispositivosAdapter(this, listDispositivo);
        listViewDispo.setAdapter(dispositivoAdapter);
        // dispositivosDB = new DispositivosDB(getApplicationContext(), "DispositivosDB.db", null, 1);

        btnVolver.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent volver = new Intent(getApplicationContext(), MenuDispositivos.class);
                startActivity(volver);
            }
        });

        iniciarFirebase();
        
        llenarListaDispo();
    }

    private void iniciarFirebase() {
        FirebaseApp.initializeApp(this);
        firebaseDatabase = FirebaseDatabase.getInstance();
        databaseReference = firebaseDatabase.getReference();
    }

    private void llenarListaDispo() {
        nombreDispositivo = new ArrayList<String>();
        idDispositivo = new ArrayList<Integer>();
        databaseReference.child("Dispositivo").addValueEventListener(new ValueEventListener() {
            @Override
            public void onDataChange(@NonNull DataSnapshot snapshot) {
                listDispositivo.clear();

                for (DataSnapshot item : snapshot.getChildren()) {
                    Dispositivo dispositivo = item.getValue(Dispositivo.class);
                    listDispositivo.add(dispositivo);
                }

                nombreDispositivo.clear();
                for (Dispositivo dispositivo : listDispositivo) {
                    nombreDispositivo.add(dispositivo.getNombre());
                }

                dispositivoAdapter.notifyDataSetChanged();
            }
            /*
            listViewDispo.setOnItemClickListener(new AdapterView.OnItemClickListener() {
                @Override
                public void onItemClick(AdapterView<?> parent, View view, int position, long id) {
                    // Aquí puedes manejar el evento de clic en el elemento de la lista
                    // Puedes obtener el nombre del dispositivo y realizar acciones según sea necesario
                    String nombreDispositivo = nombreDispositivo.get(position);
                    // ...
                }
            }); */

            @Override
            public void onCancelled(@NonNull DatabaseError error) {

            }
        });

        /*List<Dispositivo> dispositivoList = dispositivosDB.listaDispositivo();
        for (int i=0; i<dispositivoList.size(); i++) {
            Dispositivo dispositivo = dispositivoList.get(i);
            nombreDispositivo.add(dispositivo.getNombre());
            idDispositivo.add(dispositivo.getId());
        }
        listViewDispo.setOnItemClickListener(new AdapterView.OnItemClickListener() {
            @Override
            public void onItemClick(AdapterView<?> parent, View view, int position, long id) {
                Dispositivo dispositivo = dispositivosDB.elemento(idDispositivo.get(position));
                Bundle bolsa = new Bundle();
                bolsa.putInt("idDis", dispositivo.getId());
                bolsa.putString("nombreDis", dispositivo.getNombre());

                Intent intent = new Intent(getApplicationContext(), GestionarDispositivos.class);
                intent.putExtras(bolsa);
                startActivity(intent);
            }
        }); */
    }

    @Override
    public void onItemClick(String nombreDis) {
    }
}