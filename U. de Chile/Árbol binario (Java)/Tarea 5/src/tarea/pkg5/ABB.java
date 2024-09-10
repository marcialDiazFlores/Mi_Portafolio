package tarea.pkg5;
import java.lang.Math;

public class ABB implements ArbolBinario{
	private NodoABB raiz;

	public ABB(){
            raiz = null;
	}
        
        public ABB(int x){
            raiz = new NodoABB(x);
        }
        
        private class NodoABB {
            private int dato;
            private NodoABB izq, der;
            
            public NodoABB(int x){
                this.dato = x;
                this.izq = null;
                this.der = null;
            }
            
            public int getDato(){
                return dato;
            }
            
            public NodoABB getIzquierdo(){
                return izq;
            }
            
            public NodoABB getDerecho(){
                return der;
            }
            
            public void setIzquierdo(NodoABB nodo){
                izq = nodo;
            }
            
            public void setDerecho(NodoABB nodo){
                der = nodo;
            }
            
            public void setDato(int dato){
                this.dato = dato;
            }
        }
        
        private static boolean esVacio(NodoABB nodo){
            return nodo == null;
        }

	private NodoABB insertarNodo(int x, NodoABB arbol){
            NodoABB nodo = new NodoABB(x);
            if(esVacio(arbol)){
                return nodo;
            }
            else{
                if(x < arbol.getDato()){
                    arbol.setIzquierdo(insertarNodo(x, arbol.getIzquierdo()));
                    return arbol;
                }
                else{
                    arbol.setDerecho(insertarNodo(x, arbol.getDerecho()));
                    return arbol;                    
                }
            }
	}
        
        public void insertar(int x){
            raiz = insertarNodo(x, raiz);
        }
        
        private static NodoABB buscarNodo(int x, NodoABB nodo){
            if(esVacio(nodo)){
                return null;
            }
            else{
                if(x == nodo.getDato()){
                    return nodo;
                }
                else if(x < nodo.getDato()){
                    return buscarNodo(x, nodo.getIzquierdo());
                    }
                else{
                    return buscarNodo(x, nodo.getDerecho());
                }
            }
        }

	public boolean buscar(int x){
            return !esVacio(buscarNodo(x, raiz));
        }
        
        private static int alturaNodo(NodoABB nodo){
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