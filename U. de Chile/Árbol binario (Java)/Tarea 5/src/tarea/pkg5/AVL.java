package tarea.pkg5;
import java.lang.Math;

public class AVL implements ArbolBinario{
    private NodoAVL raiz;
    
    public AVL(){
        raiz = null;
    }
    
    public AVL(int x){
        raiz = new NodoAVL(x);
    }
    
    private class NodoAVL{
        private int dato, altura;
        private NodoAVL izq, der;
        
        public NodoAVL(int x){
            this.dato = x;
            this.altura = 1;
            this.izq = null;
            this.der = null;
        }
        
        public int getDato(){
            return dato;
        }
        
        public int getAltura(){
            return altura;
        }
        
        public NodoAVL getIzquierdo(){
            return izq;
        }
        
        public NodoAVL getDerecho(){
            return der;
        }
        
        public void setIzquierdo(NodoAVL nodo){
            izq = nodo;
        }
        
        public void setDerecho(NodoAVL nodo){
            der = nodo;
        }
        
        public void setAltura(int altura){
            this.altura = altura;
        }  
        
        public void setDato(int dato){
            this.dato = dato;
        }
    }
    
    private static boolean esVacio(NodoAVL nodo){
        return (nodo == null);
    }
    
    private static int altura(NodoAVL nodo){
        if(esVacio(nodo)){
            return 0;
        }
        else{
            return nodo.getAltura();
        }
    }
    
    private static NodoAVL buscarNodo(int x, NodoAVL nodo){
        if(esVacio(nodo)){
            return null;
        }
        else if(x == nodo.getDato()){
            return nodo;
        }
        else if(x < nodo.getDato()){
            return buscarNodo(x, nodo.getIzquierdo());
        }
        else{
            return buscarNodo(x, nodo.getDerecho());
        }
    }
    
    public boolean buscar(int x){
        return buscarNodo(x, raiz) != null;
    }
    
    private static NodoAVL rotacionSimpleIzquierda(NodoAVL nodo){
        NodoAVL aux = nodo.getIzquierdo();
        nodo.setIzquierdo(aux.getDerecho());
        aux.setDerecho(nodo);
        int alturaNodo = Math.max(altura(nodo.getIzquierdo()), altura(nodo.getDerecho())) + 1;
        nodo.setAltura(alturaNodo);
        int alturaAux = Math.max(altura(aux.getIzquierdo()), altura(aux.getDerecho())) + 1;
        return aux;
    }
    
    private static NodoAVL rotacionSimpleDerecha(NodoAVL nodo){
        NodoAVL aux = nodo.getDerecho();
        nodo.setDerecho(aux.getIzquierdo());
        aux.setIzquierdo(nodo);
        int alturaNodo = Math.max(altura(nodo.getIzquierdo()), altura(nodo.getDerecho())) + 1;
        nodo.setAltura(alturaNodo);
        int alturaAux = Math.max(altura(aux.getIzquierdo()), altura(aux.getDerecho())) + 1;
        return aux; 
    }
    
    private static NodoAVL rotacionDobleIzquierda(NodoAVL nodo){
        nodo.setIzquierdo(rotacionSimpleDerecha(nodo.getIzquierdo()));
        NodoAVL aux = rotacionSimpleIzquierda(nodo);
        return aux;
    }
    
    private static NodoAVL rotacionDobleDerecha(NodoAVL nodo){
        nodo.setDerecho(rotacionSimpleIzquierda(nodo.getDerecho()));
        NodoAVL aux = rotacionSimpleDerecha(nodo);
        return aux;
    }
    
    private static NodoAVL insertarNodo(NodoAVL nodo, NodoAVL arbol){
        NodoAVL aux = arbol;
        if(nodo.getDato() < arbol.getDato()){
            if(esVacio(arbol.getIzquierdo())){
                arbol.setIzquierdo(nodo);
            }
            else{
                arbol.setIzquierdo(insertarNodo(nodo, arbol.getIzquierdo()));
                if(Math.abs(altura(arbol.getIzquierdo()) - altura(arbol.getDerecho())) > 1){
                    if(nodo.getDato() < (arbol.getIzquierdo()).getDato()){
                        aux = rotacionSimpleIzquierda(arbol);
                    }
                    else{
                        aux = rotacionDobleIzquierda(arbol);
                    }
                }
            }
        }
        else if(nodo.getDato() > arbol.getDato()){
            if(esVacio(arbol.getDerecho())){
                arbol.setDerecho(nodo);
            }
            else{
                arbol.setDerecho(insertarNodo(nodo, arbol.getDerecho()));
                if(Math.abs(altura(arbol.getIzquierdo()) - altura(arbol.getDerecho())) > 1){
                    if(nodo.getDato() > (arbol.getDerecho()).getDato()){
                        aux = rotacionSimpleDerecha(arbol);
                    }
                    else{
                        aux = rotacionDobleDerecha(arbol);
                    }
                }
            }
        }
        if(esVacio(arbol.getIzquierdo()) && !esVacio(arbol.getDerecho())){
            arbol.setAltura(altura(arbol.getDerecho()) + 1);
        }
        else if(esVacio(arbol.getDerecho()) && !esVacio(arbol.getIzquierdo())){
            arbol.setAltura(altura(arbol.getIzquierdo()) + 1);
        }
        else{
            arbol.setAltura(Math.max(altura(arbol.getIzquierdo()), altura(arbol.getDerecho()) + 1));
        }
        return aux;
    }
    
    public void insertar(int x){
        NodoAVL nodo = new NodoAVL(x);
        if(esVacio(raiz)){
            raiz = nodo;
        }
        else{
            raiz = insertarNodo(nodo, raiz);
        }
    }
    
    private static int alturaNodo(NodoAVL nodo){
            if(esVacio(nodo)){
                return 0;
            }
            else{
                if(esVacio(nodo.getIzquierdo()) && esVacio(nodo.getDerecho())){
                    return 1;
                }
                else{
                    return Math.max(alturaNodo(nodo.getIzquierdo()), alturaNodo(nodo.getDerecho())) + 1;
                }
            }
        }
        
        public int altura(){
            return alturaNodo(raiz);
        }
}