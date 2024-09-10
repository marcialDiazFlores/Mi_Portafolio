package org.misproyectos.tusgastos;

import org.misproyectos.tusgastos.Modelo.Administrador;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.security.servlet.SecurityAutoConfiguration;

@SpringBootApplication(exclude = {SecurityAutoConfiguration.class})
public class TusGastosApplication {

    public static void main(String[] args) {
        SpringApplication.run(TusGastosApplication.class, args);
    }

}
