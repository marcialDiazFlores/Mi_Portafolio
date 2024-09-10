package com.example.energysaver;

import androidx.annotation.NonNull;
import androidx.appcompat.app.AppCompatActivity;

import android.content.Intent;
import android.os.Bundle;
import android.text.TextUtils;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;
import android.widget.Toast;

import com.google.android.gms.tasks.OnCompleteListener;
import com.google.android.gms.tasks.Task;
import com.google.firebase.Firebase;
import com.google.firebase.FirebaseApp;
import com.google.firebase.auth.AuthResult;
import com.google.firebase.auth.FirebaseAuth;
import com.google.firebase.auth.FirebaseUser;
import com.google.firebase.database.DatabaseReference;
import com.google.firebase.database.FirebaseDatabase;

public class Registrar extends AppCompatActivity {

    EditText emailEd, passwordEd;
    Button btnRegistrar;
    TextView login;
    FirebaseAuth mAuth;

    @Override
    public void onStart() {
        super.onStart();
        FirebaseUser currentUser = mAuth.getCurrentUser();
        if (currentUser != null) {
            Intent menu = new Intent(getApplicationContext(), MenuInicial.class);
            startActivity(menu);
            finish();
        }
    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_registrar);

        emailEd = findViewById(R.id.email);
        passwordEd = findViewById(R.id.password);
        btnRegistrar = findViewById(R.id.btnRegistrar);
        login = findViewById(R.id.login);
        mAuth = FirebaseAuth.getInstance();
        login = findViewById(R.id.login);

        login.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent login = new Intent(getApplicationContext(), Login.class);
                startActivity(login);
                finish();
            }
        });

        btnRegistrar.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                String email, password;
                email = String.valueOf(emailEd.getText());
                password = String.valueOf(passwordEd.getText());

                if (TextUtils.isEmpty(email)) {
                    Toast.makeText(Registrar.this, "Ingrese su correo electronico.", Toast.LENGTH_SHORT).show();
                    return;
                }

                if (TextUtils.isEmpty(password)) {
                    Toast.makeText(Registrar.this, "Ingrese su contraseña.", Toast.LENGTH_SHORT).show();
                    return;
                }

                mAuth.createUserWithEmailAndPassword(email, password)
                        .addOnCompleteListener(new OnCompleteListener<AuthResult>() {
                            @Override
                            public void onComplete(@NonNull Task<AuthResult> task) {
                                if (task.isSuccessful()) {
                                    Toast.makeText(Registrar.this, "Cuenta creada exitosamente", Toast.LENGTH_SHORT).show();
                                    Intent login = new Intent(getApplicationContext(), Login.class);
                                    startActivity(login);
                                    finish();
                                } else {
                                    // If sign in fails, display a message to the user.
                                    Toast.makeText(Registrar.this, "Error en la autenticación.", Toast.LENGTH_SHORT).show();
                                }
                            }
                        });
            }
        });
    }
}