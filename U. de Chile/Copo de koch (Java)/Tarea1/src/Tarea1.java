package Tarea1.src;

import java.util.Scanner;

public class Tarea1 {
    static int LMIN = 5;                                                         // Largo mínimo
    public static void segmentoKoch(Turtle tortuga, int largo) {                 // Función que dibuja un segmento del copo de Koch
        if (largo < LMIN)                                                        // Caso base
        {
            tortuga.goForward(largo);                                            // La tortuga avanza una cantidad largo hacia adelante si largo es menor al minimo
        }
        else                                                                     // Recursión: En caso contrario se dibujan cuatro segmentos de Koch más pequeños (largo/3)
        {
            segmentoKoch(tortuga, largo/3);
            tortuga.turnLeft(60);
            segmentoKoch(tortuga, largo/3);
            tortuga.turnLeft(240);
            segmentoKoch(tortuga, largo/3);
            tortuga.turnLeft(60);
            segmentoKoch(tortuga, largo/3);            
        }
    }
    public static void curvaDeKoch(Turtle tortuga, int largo) {                  // Función que dibuja los tres segmentos del copo de Koch
        segmentoKoch(tortuga, largo);
        tortuga.turnLeft(240);
        segmentoKoch(tortuga, largo);
        tortuga.turnLeft(240);
        segmentoKoch(tortuga, largo);
    }    
    public static void main(String[ ] args){                                     // Función principal que dibuja un copo de largo 650, y pide al usuario el ángulo de inclinación del copo
       int largo = 650;
       System.out.println("Ingrese el angulo alfa con el"
               + " que desea rotar el copo de Koch");
       System.out.println("(El copo sera rotado desde el"
               + " centro con un angulo alfa respecto a"
               + " la horizontal, en sentido antihorario)");
       Scanner lector = new Scanner(System.in);
       int alfa = lector.nextInt();
       Turtle tortuga;
       tortuga = new Turtle(350, 289, 180 + alfa, 700, 578);                     // La tortuga parte en el centro y se va a la posicion de dibujo (según el ángulo de inclinación)
       tortuga.setPenUp();
       tortuga.goForward(247);
       tortuga.turnLeft(270);
       tortuga.goForward(143);
       tortuga.turnLeft(270);
       tortuga.setPenDown();
       curvaDeKoch(tortuga, largo);       
    }
}