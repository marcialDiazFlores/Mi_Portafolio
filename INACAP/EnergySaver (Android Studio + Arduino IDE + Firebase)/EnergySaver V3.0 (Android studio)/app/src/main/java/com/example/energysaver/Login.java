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
import com.google.firebase.auth.AuthResult;
import com.google.firebase.auth.FirebaseAuth;
import com.google.firebase.auth.FirebaseUser;

import org.w3c.dom.Text;

public class Login extends AppCompatActivity {

    EditText  emailEd, passwordEd;
    Button btnLog;
    TextView registrar;
    FirebaseAuth mAuth;

    @Override
    public void onStart() {
        super.onStart();
        FirebaseUser currentUser = mAuth.getCurrentUser();
        if(currentUser != null){
            Intent menu = new Intent(getApplicationContext(), MenuInicial.class);
            startActivity(menu);
            finish();
        }
    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_login);

        emailEd = findViewById(R.id.email);
        passwordEd = findViewById(R.id.password);
        btnLog = findViewById(R.id.btnLog);
        registrar = findViewById(R.id.registrar);
        mAuth = FirebaseAuth.getInstance();

        registrar.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent register = new Intent(getApplicationContext(), Registrar.class);
                startActivity(register);
                finish();
            }
        });

        btnLog.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                String email, password;
                email = String.valueOf(emailEd.getText());
                password = String.valueOf(passwordEd.getText());

                if (TextUtils.isEmpty(email)) {
                    Toast.makeText(Login.this, "Ingrese su correo electronico.", Toast.LENGTH_SHORT).show();
                    return;
                }

                if (TextUtils.isEmpty(password)) {
                    Toast.makeText(Login.this, "Ingrese su contraseña.", Toast.LENGTH_SHORT).show();
                    return;
                }

                mAuth.signInWithEmailAndPassword(email, password)
                        .addOnCompleteListener(new OnCompleteListener<AuthResult>() {
                            @Override
                            public void onComplete(@NonNull Task<AuthResult> task) {
                                if (task.isSuccessful()) {
                                    Toast.makeText(getApplicationContext(), "Inicio de sesión exitoso", Toast.LENGTH_SHORT).show();
                                    Intent menu = new Intent(getApplicationContext(), MenuInicial.class);
                                    startActivity(menu);
                                    finish();
                                } else {
                                    Toast.makeText(Login.this, "Error de correo o contraseña.", Toast.LENGTH_SHORT).show();
                                }
                            }
                        });
            }
        });
    }
}