package tarea.pkg5;

public class ARN implements ArbolBinario{
    private static final boolean rojo = true;
    private static final boolean negro = false;
    private NodoARN raiz;
    
    public ARN(){
    }
    
    public ARN(int x){
        raiz = new NodoARN(x, negro, 1);
    }
    
    public class NodoARN {
        public int dato;
        public NodoARN izq, der;
        public boolean color;
        public int N;             

        public NodoARN(int dato, boolean color, int N) {
            this.dato = dato;
            this.color = color;
            this.N = N;
        }
    }
    
    private boolean esRojo(NodoARN nodo) {
        if (nodo == null){
            return false;
        }
        else{
            return nodo.color == rojo;
        }
    }
    
    private int tamaño(NodoARN nodo) {
        if (nodo == null){
            return 0;
        }
        else{
            return nodo.N;
        }
    }
    
    private static boolean esVacio(NodoARN nodo) {
        return nodo == null;
    }
    
    public boolean buscar(int x) {
        return !esVacio(buscarNodo(x, raiz));
    }
    
    private NodoARN buscarNodo(int x, NodoARN nodo){
        if(esVacio(nodo)){
            return null;
        }
        else if(x == nodo.dato){
            return nodo;
        }
        else if(x < nodo.dato){
            return buscarNodo(x, nodo.izq);
        }
        else if(x > nodo.dato){
            return buscarNodo(x, nodo.der);
        }
        return null;
    }
    
    public void insertar(int x) {
        raiz = insertar(raiz, x);
        raiz.color = negro;
    }
    
    private NodoARN insertar(NodoARN nodo, int x) { 
        if(nodo == null){
            return new NodoARN(x, rojo, 1);
        }
        else if(x < nodo.dato){
            nodo.izq  = insertar(nodo.izq, x);
        } 
        else if(x > nodo.dato){
            nodo.der = insertar(nodo.der, x);
        }
        if(esRojo(nodo.der) && !esRojo(nodo.izq)){
            nodo = rotacionSimpleIzquierda(nodo);
        }
        if(esRojo(nodo.izq)  &&  esRojo(nodo.izq.izq)){
            nodo = rotacionSimpleDerecha(nodo);
        }
        if(esRojo(nodo.izq)  &&  esRojo(nodo.der)){
            cambiarColores(nodo);
        }
        nodo.N = tamaño(nodo.izq) + tamaño(nodo.der) + 1;
        return nodo;
    }
    
    private NodoARN rotacionSimpleDerecha(NodoARN nodo) {
        NodoARN aux = nodo.izq;
        nodo.izq = aux.der;
        aux.der = nodo;
        aux.color = aux.der.color;
        aux.der.color = rojo;
        aux.N = nodo.N;
        nodo.N = tamaño(nodo.izq) + tamaño(nodo.der) + 1;
        return aux;
    }
    
    private NodoARN rotacionSimpleIzquierda(NodoARN nodo){
        NodoARN aux = nodo.der;
        nodo.der = aux.izq;
        aux.izq = nodo;
        aux.color = aux.izq.color;
        aux.izq.color = rojo;
        aux.N = nodo.N;
        nodo.N = tamaño(nodo.izq) + tamaño(nodo.der) + 1;
        return aux;
    }
    
    private void cambiarColores(NodoARN nodo) {
        nodo.color = !nodo.color;
        nodo.izq.color = !nodo.izq.color;
        nodo.der.color = !nodo.der.color;
    }
    
    private static int alturaNodo(NodoARN nodo){
            if(esVacio(nodo)){
                return 0;
            }
            else{
                if(esVacio(nodo.izq) && esVacio(nodo.der)){
                    return 1;
                }
                else{
                    return Math.max(alturaNodo(nodo.izq), alturaNodo(nodo.der)) + 1;
                }
            }
        }
        
        public int altura(){
            return alturaNodo(raiz);
        }
}