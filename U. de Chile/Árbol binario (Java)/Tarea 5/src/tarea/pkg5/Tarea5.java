package tarea.pkg5;
import java.util.Random;
import java.lang.Math;
import java.util.Calendar;

public class Tarea5{
    
    public static void main(String[] args){       
        Random rnd = new Random();
        System.out.println("INSERCIONES Y BUSQUEDAS");
        System.out.println("");
        System.out.println("Distribucion ascendente");
        double n;
        for(n = 1000; n <= 10000; n = n + 1000){
            ABB arbolABB = new ABB();
            AVL arbolAVL = new AVL();
            ARN arbolARN = new ARN();
            System.out.println("");
            System.out.println("n = " + (int)n);
            System.out.println("");
            long tiempoInicial = System.nanoTime();
            for(int i = 1; i <= n; i++){
                arbolABB.insertar(i);
            }
            long tiempoFinal = System.nanoTime();
            double tiempo = (double) tiempoFinal - tiempoInicial;
            double tiempoPromedio = tiempo/n;
            System.out.println("Tiempo promedio insercion ABB: " + tiempoPromedio + " nanosegundos.");
            float log = (float) Math.log(n);
            boolean cota = (tiempoPromedio/1000.0 < log);
            if(cota){
                System.out.println("Arbol ABB cumple la cota logaritmica (insercion) para n = " + (int)n + ".");
            }
            else{
                System.out.println("Arbol ABB no cumple la cota logaritmica (insercion) para n = " + (int)n + ".");
            }
            int altura = arbolABB.altura();
            System.out.println("Altura ABB = " + altura);
            long tiempoInicialB = System.nanoTime();
            for(int i = 1; i <= 1000; i++){
                arbolABB.buscar(rnd.nextInt());
            }
            long tiempoFinalB = System.nanoTime();
            double tiempoB = (double) tiempoFinalB - tiempoInicialB;
            double tiempoPromedioB = tiempoB/1000.0;
            System.out.println("Tiempo promedio busqueda ABB: " + tiempoPromedioB + " nanosegundos.");
            boolean cotaB = (tiempoPromedioB/1000.0 < log);
            if(cotaB){
                System.out.println("Arbol ABB cumple la cota logaritmica (busqueda) para n = " + (int)n + ".");
            }
            else{
                System.out.println("Arbol ABB no cumple la cota logaritmica (busqueda) para n = " + (int)n + ".");
            }
            System.out.println("");
            long tiempoInicial2 = System.nanoTime();
            for(int i = 1; i <= n; i++){
                arbolAVL.insertar(i);
            }
            long tiempoFinal2 = System.nanoTime();
            double tiempo2 = (double) tiempoFinal2 - tiempoInicial2;
            double tiempoPromedio2 = tiempo2/n;
            System.out.println("Tiempo promedio insercion AVL: " + tiempoPromedio2 + " nanosegundos.");
            float log2 = (float) Math.log(n);
            boolean cota2 = (tiempoPromedio2/1000.0 < log2);
            if(cota2){
                System.out.println("Arbol AVL cumple la cota logaritmica (insercion) para n = " + (int)n + ".");
            }
            else{
                System.out.println("Arbol AVL no cumple la cota logaritmica (insercion) para n = " + (int)n + ".");
            }
            int altura2 = arbolAVL.altura();
            System.out.println("Altura AVL = " + altura2);
            long tiempoInicial2B = System.nanoTime();
            for(int i = 1; i <= 1000; i++){
                arbolAVL.buscar(rnd.nextInt());
            }
            long tiempoFinal2B = System.nanoTime();
            double tiempo2B = (double) tiempoFinal2B - tiempoInicial2B;
            double tiempoPromedio2B = tiempo2B/1000.0;
            System.out.println("Tiempo promedio busqueda AVL: " + tiempoPromedio2B + " nanosegundos.");
            boolean cota2B = (tiempoPromedio2B/1000.0 < log);
            if(cota2B){
                System.out.println("Arbol AVL cumple la cota logaritmica (busqueda) para n = " + (int)n + ".");
            }
            else{
                System.out.println("Arbol ABB no cumple la cota logaritmica (busqueda) para n = " + (int)n + ".");
            }
            System.out.println("");
            long tiempoInicial3 = System.nanoTime();
            for(int i = 1; i <= n; i++){
                arbolARN.insertar(i);
            }
            long tiempoFinal3 = System.nanoTime();
            double tiempo3 = (double) tiempoFinal3 - tiempoInicial3;
            double tiempoPromedio3 = tiempo3/n;
            System.out.println("Tiempo promedio insercion Arbol Rojo-Negro: " + tiempoPromedio3 + " nanosegundos.");
            float log3 = (float) Math.log(n);
            boolean cota3 = (tiempoPromedio3/1000.0 < log3);
            if(cota3){
                System.out.println("Arbol Rojo-Negro cumple la cota logaritmica (insercion) para n = " + (int)n + ".");
            }
            else{
                System.out.println("Arbol Rojo-Negro no cumple la cota logaritmica (insercion) para n = " + (int)n + ".");
            }
            int altura3 = arbolARN.altura();
            System.out.println("Altura Arbol Rojo-Negro = " + altura3);
            long tiempoInicial3B = System.nanoTime();
            for(int i = 1; i <= 1000; i++){
                arbolARN.buscar(rnd.nextInt());
            }
            long tiempoFinal3B = System.nanoTime();
            double tiempo3B = (double) tiempoFinal3B - tiempoInicial3B;
            double tiempoPromedio3B = tiempo3B/1000.0;
            System.out.println("Tiempo promedio busqueda Arbol Rojo-Negro: " + tiempoPromedio3B + " nanosegundos.");
            boolean cota3B = (tiempoPromedio3B/1000.0 < log);
            if(cota3B){
                System.out.println("Arbol ABB cumple la cota logaritmica (busqueda) para n = " + (int)n + ".");
            }
            else{
                System.out.println("Arbol ABB no cumple la cota logaritmica (busqueda) para n = " + (int)n + ".");
            }
        }
        System.out.println("");
        System.out.println("Distribucion al azar");
        for(n = 1000; n <= 10000; n = n + 1000){
            ABB arbolABB = new ABB();
            AVL arbolAVL = new AVL();
            ARN arbolARN = new ARN();
            System.out.println("");
            System.out.println("n = " + (int)n);
            System.out.println("");
            long tiempoInicial = System.nanoTime();
            for(int i = 1; i <= n; i++){
                arbolABB.insertar(rnd.nextInt(1000000));
            }
            long tiempoFinal = System.nanoTime();
            double tiempo = (double) tiempoFinal - tiempoInicial;
            double tiempoPromedio = tiempo/n;
            System.out.println("Tiempo promedio insercion ABB: " + tiempoPromedio + " nanosegundos.");
            float log = (float) Math.log(n);
            boolean cota = (tiempoPromedio/1000.0 < log);
            if(cota){
                System.out.println("Arbol ABB cumple la cota logaritmica (insercion) para n = " + (int)n + ".");
            }
            else{
                System.out.println("Arbol ABB no cumple la cota logaritmica (insercion) para n = " + (int)n + ".");
            }
            int altura = arbolABB.altura();
            System.out.println("Altura ABB = " + altura);
            long tiempoInicialB = System.nanoTime();
            for(int i = 1; i <= 1000; i++){
                arbolABB.buscar(rnd.nextInt());
            }
            long tiempoFinalB = System.nanoTime();
            double tiempoB = (double) tiempoFinalB - tiempoInicialB;
            double tiempoPromedioB = tiempoB/1000.0;
            System.out.println("Tiempo promedio busqueda ABB: " + tiempoPromedioB + " nanosegundos.");
             boolean cotaB = (tiempoPromedioB/1000.0 < log);
            if(cotaB){
                System.out.println("Arbol ABB cumple la cota logaritmica (busqueda) para n = " + (int)n + ".");
            }
            else{
                System.out.println("Arbol ABB no cumple la cota logaritmica (busqueda) para n = " + (int)n + ".");
            }
            System.out.println("");
            long tiempoInicial2 = System.nanoTime();
            for(int i = 1; i <= n; i++){
                arbolAVL.insertar(rnd.nextInt(1000000000));
            }
            long tiempoFinal2 = System.nanoTime();
            double tiempo2 = (double) tiempoFinal2 - tiempoInicial2;
            double tiempoPromedio2 = tiempo2/n;
            System.out.println("Tiempo promedio insercion AVL: " + tiempoPromedio2 + " nanosegundos.");
            float log2 = (float) Math.log(n);
            boolean cota2 = (tiempoPromedio2/1000.0 < log2);
            if(cota2){
                System.out.println("Arbol AVL cumple la cota logaritmica (insercion) para n = " + (int)n + ".");
            }
            else{
                System.out.println("Arbol AVL no cumple la cota logaritmica (insercion) para n = " + (int)n + ".");
            }
            int altura2 = arbolAVL.altura();
            System.out.println("Altura AVL = " + altura2);
            long tiempoInicial2B = System.nanoTime();
            for(int i = 1; i <= 1000; i++){
                arbolAVL.buscar(rnd.nextInt());
            }
            long tiempoFinal2B = System.nanoTime();
            double tiempo2B = (double) tiempoFinal2B - tiempoInicial2B;
            double tiempoPromedio2B = tiempo2B/1000.0;
            System.out.println("Tiempo promedio busqueda AVL: " + tiempoPromedio2B + " nanosegundos.");
            boolean cota2B = (tiempoPromedio2B/1000.0 < log);
            if(cota2B){
                System.out.println("Arbol AVL cumple la cota logaritmica (busqueda) para n = " + (int)n + ".");
            }
            else{
                System.out.println("Arbol ABB no cumple la cota logaritmica (busqueda) para n = " + (int)n + ".");
            }
            System.out.println("");
            long tiempoInicial3 = System.nanoTime();
            for(int i = 1; i <= n; i++){
                arbolARN.insertar(rnd.nextInt(1000000000));
            }
            long tiempoFinal3 = System.nanoTime();
            double tiempo3 = (double) tiempoFinal3 - tiempoInicial3;
            double tiempoPromedio3 = tiempo3/n;
            System.out.println("Tiempo promedio insercion Arbol Rojo-Negro: " + tiempoPromedio3 + " nanosegundos.");
            float log3 = (float) Math.log(n);
            boolean cota3 = (tiempoPromedio3/1000.0 < log3);
            if(cota3){
                System.out.println("Arbol Rojo-Negro cumple la cota logaritmica (insercion) para n = " + (int)n + ".");
            }
            else{
                System.out.println("Arbol Rojo-Negro no cumple la cota logaritmica (insercion) para n = " + (int)n + ".");
            }
            int altura3 = arbolARN.altura();
            System.out.println("Altura Arbol Rojo-Negro = " + altura3);
            long tiempoInicial3B = System.nanoTime();
            for(int i = 1; i <= 1000; i++){
                arbolARN.buscar(rnd.nextInt());
            }
            long tiempoFinal3B = System.nanoTime();
            double tiempo3B = (double) tiempoFinal3B - tiempoInicial3B;
            double tiempoPromedio3B = tiempo3B/1000.0;
            System.out.println("Tiempo promedio busqueda Arbol Rojo-Negro: " + tiempoPromedio3B + " nanosegundos.");
            boolean cota3B = (tiempoPromedio3B/1000.0 < log);
            if(cota3B){
                System.out.println("Arbol ABB cumple la cota logaritmica (busqueda) para n = " + (int)n + ".");
            }
            else{
                System.out.println("Arbol ABB no cumple la cota logaritmica (busqueda) para n = " + (int)n + ".");
            }
        }
    }
}
