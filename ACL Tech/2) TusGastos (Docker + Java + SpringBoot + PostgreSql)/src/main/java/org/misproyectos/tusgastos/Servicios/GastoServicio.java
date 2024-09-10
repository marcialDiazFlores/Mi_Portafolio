package org.misproyectos.tusgastos.Servicios;

import org.misproyectos.tusgastos.Excepciones.GastoNoEncontradoException;
import org.misproyectos.tusgastos.Modelo.Gasto;
import org.misproyectos.tusgastos.Repositorios.GastoRepositorio;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
public class GastoServicio {
    @Autowired private GastoRepositorio repositorio;

    public List<Gasto> listarTodos(int idUsuario) {
        return repositorio.findAllOrderByFechaAsc(idUsuario);
    }

    public void agregar(Gasto gasto) {
        repositorio.save(gasto);
    }

    public Gasto buscar(Integer id) throws GastoNoEncontradoException {
        Optional<Gasto> resultado = repositorio.findById(id);
        if (resultado.isPresent()) {
            return resultado.get();
        }
        throw new GastoNoEncontradoException("No se encontró un gasto con el ID " + id);
    }

    public void eliminar(Integer id) throws GastoNoEncontradoException {
        Integer buscar = repositorio.countById(id);
        if (buscar == null || buscar == 0) {
            throw new GastoNoEncontradoException("No se encontró un gasto fijo con el ID " + id);
        }
        repositorio.deleteById(id);
    }
}
