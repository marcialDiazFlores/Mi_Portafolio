package com.example.energysaver;

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

public class GestionarDispositivos extends AppCompatActivity {

    private static int contadorId = 0;
    Button btnVolver, btnGuardar, btnEditar, btnEliminar;
    EditText gestionDispo;
    int id;
    //DispositivosDB dispositivosDB;
    FirebaseDatabase firebaseDatabase;
    DatabaseReference databaseReference;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_gestionar_dispositivos);

        btnVolver = findViewById(R.id.btnVolver);
        btnGuardar = findViewById(R.id.btnGuardarDispo);
        btnEditar = findViewById(R.id.btnEditarDispo);
        btnEliminar = findViewById(R.id.btnEliminarDispo);
        gestionDispo = findViewById(R.id.gestionDispo);

        iniciarFirebase();

        DatabaseReference dispositivosRef = databaseReference.child("Dispositivo");

        btnVolver.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent volver = new Intent(getApplicationContext(), MenuDispositivos.class);
                startActivity(volver);
            }
        });

        btnEditar.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                guardarDispo();
            }
        });

        btnGuardar.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                guardarDispo();
            }
        });

        btnEliminar.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                borrarDispo();
            }
        });

        Intent i = getIntent();
        Bundle bolsa = i.getExtras();
        id = bolsa.getInt("idDis");
        if (id != 0){
            gestionDispo.setText(bolsa.getString("nombreDis"));
            btnGuardar.setEnabled(false);
        } else {
            btnEditar.setEnabled(true);
            btnEliminar.setEnabled(true);
        }
    }

    private void iniciarFirebase() {
        FirebaseApp.initializeApp(this);
        firebaseDatabase = FirebaseDatabase.getInstance();
        databaseReference = firebaseDatabase.getReference();
    }

    private void limparCamposDis() {
        id = 0;
        gestionDispo.setText("");
    }

    private Dispositivo llenarDatosDispo() {
        Dispositivo dispositivo = new Dispositivo();
        //Dispositivo dispositivo = new Dispositivo();
        String nombreDis = gestionDispo.getText().toString();

        //dispositivo.setId(contadorId++);
        dispositivo.setNombre(nombreDis);
        dispositivo.setEncendido(false);
        return dispositivo;
    }
    private void guardarDispo() {
        //dispositivosDB = new DispositivosDB(getApplicationContext(), "DispositivosDB.db", null, 1);
        Dispositivo dispositivo = llenarDatosDispo();
        if (id == 0) {
            //dispositivosDB.agregarDis(dispositivo);
            databaseReference.child("Dispositivo").child(dispositivo.getNombre()).setValue(dispositivo);
            Toast.makeText(this, "Dispositivo Registrado", Toast.LENGTH_SHORT).show();
        } else {
            //dispositivosDB.actualizarDis(id, dispositivo);
            btnEditar.setEnabled(true);
            btnEliminar.setEnabled(true);
            Toast.makeText(this, "Dispositivo Actualizado", Toast.LENGTH_SHORT).show();
        }
    }

    private void borrarDispo() {
        Dispositivo dispositivo = llenarDatosDispo();
        if (id == 0) {
            Toast.makeText(this, "No es posible eliminar este dispositivo", Toast.LENGTH_SHORT).show();
        } else {
            databaseReference.child("Dispositivos").child(String.valueOf(id)).removeValue();
            limparCamposDis();
            btnGuardar.setEnabled(true);
            btnEliminar.setEnabled(false);
            btnEditar.setEnabled(false);
            Toast.makeText(this, "Dispositivo Eliminado", Toast.LENGTH_SHORT).show();
        }
    }
}