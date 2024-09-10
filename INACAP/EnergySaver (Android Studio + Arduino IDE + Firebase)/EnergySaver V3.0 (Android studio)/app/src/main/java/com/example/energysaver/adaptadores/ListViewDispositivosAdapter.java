package com.example.energysaver.adaptadores;

import android.content.Context;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.BaseAdapter;
import android.widget.Switch;
import android.widget.TextView;

import com.example.energysaver.R;
import com.example.energysaver.modelos.Dispositivo;

import android.widget.CompoundButton;
import android.widget.Switch;
import android.widget.TextView;

import com.google.firebase.database.DatabaseReference;
import com.google.firebase.database.FirebaseDatabase;

import java.util.ArrayList;
import java.util.List;

public class ListViewDispositivosAdapter extends BaseAdapter {

    Context context;
    List<Dispositivo> dispositivoArrayList;
    LayoutInflater layoutInflater;
    Dispositivo dispositivo;

    public ListViewDispositivosAdapter(Context context, List<Dispositivo> dispositivoArrayList) {
        this.context = context;
        this.dispositivoArrayList = dispositivoArrayList;
        layoutInflater = (LayoutInflater) context.getSystemService(
                Context.LAYOUT_INFLATER_SERVICE
        );
    }

    @Override
    public int getCount() {
        return dispositivoArrayList.size();
    }

    @Override
    public Object getItem(int position) {
        return dispositivoArrayList.get(position);
    }

    @Override
    public long getItemId(int position) {
        return 0;
    }

    @Override
    public View getView(int position, View convertView, ViewGroup parent) {
        View rowView = convertView;
        ViewHolder holder;

        if (rowView == null) {
            // Inflar el diseño list_item_dispositivo.xml
            rowView = layoutInflater.inflate(R.layout.list_item_dispositivo, parent, false);

            // Crear un ViewHolder y almacenarlo como una etiqueta en la vista
            holder = new ViewHolder();
            holder.textNombreDispositivo = rowView.findViewById(R.id.textNombreDispositivo);
            holder.switchControlDispositivo = rowView.findViewById(R.id.switchControlDispositivo);
            holder.switchControlMovimiento = rowView.findViewById(R.id.switchControlMovimiento);
            rowView.setTag(holder);
        } else {
            // La vista ya está inflada, recuperar el ViewHolder almacenado en la etiqueta
            holder = (ViewHolder) rowView.getTag();
        }

        // Configurar los datos en la vista
        Dispositivo dispositivo = dispositivoArrayList.get(position);
        holder.textNombreDispositivo.setText(dispositivo.getNombre());

        holder.switchControlDispositivo.setOnCheckedChangeListener(new CompoundButton.OnCheckedChangeListener() {
            @Override
            public void onCheckedChanged(CompoundButton buttonView, boolean isChecked) {
                // Actualiza el estado del dispositivo en la lista y en la base de datos
                dispositivo.setEncendido(isChecked);
                actualizarEstadoEnFirebase(dispositivo);
            }
        });

        holder.switchControlMovimiento.setOnCheckedChangeListener(new CompoundButton.OnCheckedChangeListener() {
            @Override
            public void onCheckedChanged(CompoundButton buttonView, boolean isChecked) {
                // Actualiza el estado del dispositivo en la lista y en la base de datos
                dispositivo.setMovimiento(isChecked);
                actualizarEstadoEnFirebase(dispositivo);
            }
        });

        return rowView;
    }

    private void actualizarEstadoEnFirebase(Dispositivo dispositivo) {
        // Obtén una referencia a la ubicación del dispositivo en la base de datos
        DatabaseReference dispositivoRef = FirebaseDatabase.getInstance().getReference()
                .child("Dispositivo").child(dispositivo.getNombre());

        // Actualiza el valor del atributo "encendido" en la base de datos
        dispositivoRef.child("encendido").setValue(dispositivo.isEncendido());

        // Actualiza el valor del atributo "movimiento" en la base de datos
        dispositivoRef.child("movimiento").setValue(dispositivo.isMovimiento());
    }

    // ViewHolder para almacenar las vistas
    static class ViewHolder {
        TextView textNombreDispositivo;
        Switch switchControlDispositivo;
        Switch switchControlMovimiento;
    }

}