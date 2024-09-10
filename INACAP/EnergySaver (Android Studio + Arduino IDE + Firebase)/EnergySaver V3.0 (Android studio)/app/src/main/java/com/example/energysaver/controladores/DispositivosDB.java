/*package com.example.energysaver.controladores;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.util.Log;

import androidx.annotation.Nullable;

import com.example.energysaver.modelos.Dispositivo;

import java.util.ArrayList;
import java.util.List;

public class DispositivosDB extends SQLiteOpenHelper implements IDispositivosDB{
    Context contexto;
    private List<Dispositivo> dispositivoList = new ArrayList<>();

    public DispositivosDB(@Nullable Context context, @Nullable String name, @Nullable SQLiteDatabase.CursorFactory factory, int version) {
        super(context, name, factory, version);
        this.contexto = context;
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        String sql = "CREATE TABLE dispositivos (" +
                "_id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                "nombreDis TEXT)";
        db.execSQL(sql);
        String insert = "INSERT INTO dispositivos VALUES (null, " + "'Ampolleta Cocina')";
        db.execSQL(insert);
        insert = "INSERT INTO dispositivos VALUES (null, " + "'Ventilador Pieza')";
        db.execSQL(insert);
        insert = "INSERT INTO dispositivos VALUES (null, " + "'Sensor Jardin')";
        db.execSQL(insert);
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
    }

    @Override
    public Dispositivo elemento(int id) {
        SQLiteDatabase database = getReadableDatabase();
        String sql = "SELECT * FROM dispositivos WHERE _id=" + id;
        Cursor cursor = database.rawQuery(sql, null);
        try {
            if (cursor.moveToNext()) {
                return extraerDis(cursor);
            } else {
                return null;
            }
        } catch (Exception e) {
            Log.d("TAG", "Error elemento (id) DispositivoDB" + e.getMessage());
            throw e;
        } finally {
            if (cursor != null) cursor.close();
        }
    }

    private Dispositivo extraerDis(Cursor cursor) {
         Dispositivo dispositivo = new Dispositivo();
         //dispositivo.setId(cursor.getInt(0));
         dispositivo.setNombre(cursor.getString(1));

         return dispositivo;
    }

    @Override
    public Dispositivo elemento(String nombre) {
        SQLiteDatabase database = getReadableDatabase();
        String sql = "SELECT * FROM dispositivos WHERE nombreDis='" + nombre + "'";
        Cursor cursor = database.rawQuery(sql, null);
        try {
            if (cursor.moveToNext()) {
                return extraerDis(cursor);
            } else {
                return null;
            }
        } catch (Exception e) {
            Log.d("TAG", "Error elemento (nombreDis) DispositivoDB" + e.getMessage());
            throw e;
        } finally {
            if (cursor != null) cursor.close();
        }
    }

    @Override
    public List<Dispositivo> listaDispositivo() {
        SQLiteDatabase database = getReadableDatabase();
        String sql = "SELECT * FROM dispositivos ORDER BY nombreDis ASC";
        Cursor cursor = database.rawQuery(sql, null);
        if (cursor.moveToFirst()) {
            do {
                dispositivoList.add(
                        new Dispositivo(cursor.getInt(0),
                                cursor.getString(1))
                );
            } while(cursor.moveToNext());
        }
        cursor.close();
        return dispositivoList;
    }

    @Override
    public void agregarDis(Dispositivo device) {
        SQLiteDatabase database = getReadableDatabase();
        ContentValues values = new ContentValues();
        values.put("nombreDis", device.getNombre());
        database.insert("dispositivos", null, values);
    }

    @Override
    public void actualizarDis(int idDispositivo, Dispositivo device) {
        SQLiteDatabase database = getReadableDatabase();
        String[] parametros = { String.valueOf(idDispositivo)};
        ContentValues values = new ContentValues();
        values.put("nombreDis", device.getNombre());
        database.update("dispositivos", values, "_id=?", parametros);
    }

    @Override
    public void eliminarDis(int idDispositivo) {
        SQLiteDatabase database = getReadableDatabase();
        String[] parametros = { String.valueOf(idDispositivo)};
        database.delete("dispositivos", "_id=?", parametros);
    }
}*/