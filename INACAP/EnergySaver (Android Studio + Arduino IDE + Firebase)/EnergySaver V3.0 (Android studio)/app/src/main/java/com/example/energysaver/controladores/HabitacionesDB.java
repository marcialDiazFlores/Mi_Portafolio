package com.example.energysaver.controladores;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.util.Log;

import com.example.energysaver.modelos.Habitacion;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

public class HabitacionesDB extends SQLiteOpenHelper implements IHabitacionesDB {
    Context contexto;
    private List<Habitacion> habitacionesList = new ArrayList<>();

    public HabitacionesDB(@Nullable Context context, @Nullable String name, @Nullable SQLiteDatabase.CursorFactory factory, int version) {
        super(context, name, factory, version);
        this.contexto = context;
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        String sql = "CREATE TABLE habitaciones (" +
                "_id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                "nombre TEXT)";
        db.execSQL(sql);
        String insert = "INSERT INTO habitaciones VALUES (null, " +"'Cocina')";
        db.execSQL(insert);
        insert = "INSERT INTO habitaciones VALUES (null, " +"'Baño')";
        db.execSQL(insert);
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
    }

    @Override
    public Habitacion elemento(int id) {
        SQLiteDatabase database = getReadableDatabase();
        String sql = "SELECT * FROM habitaciones WHERE _id = " + id;
        Cursor cursor = database.rawQuery(sql, null);
        try {
            if (cursor.moveToNext())
                return extraerHabitacion(cursor);
            else
                return null;
        } catch (Exception exception) {
            Log.d("TAG", "Error elemento(id) HabitacionesDB" + exception.getMessage());
            throw exception;
        } finally {
            if (cursor != null) cursor.close();
        }
    }

    private Habitacion extraerHabitacion(Cursor cursor) {
        Habitacion habitacion = new Habitacion ();
        habitacion.setId(cursor.getInt(0));
        habitacion.setNombre(cursor.getString(1));
        return habitacion;
    }

    @Override
    public Habitacion elemento(String nombre) {
        SQLiteDatabase database = getReadableDatabase();
        String sql = "SELECT * FROM habitaciones WHERE nombre = '"+ nombre + "'";
        Cursor cursor = database.rawQuery(sql, null);
        try {
            if (cursor.moveToNext())
                return extraerHabitacion(cursor);
            else
                return null;
        } catch (Exception exception) {
            Log.d("TAG", "Error elemento(nombre) HabitacionesDB" + exception.getMessage());
            throw exception;
        } finally {
            if (cursor != null) cursor.close();
        }
    }

    @Override
    public List<Habitacion> lista() {
        SQLiteDatabase database = getReadableDatabase();
        String sql = "SELECT * FROM habitaciones ORDER BY nombre ASC";
        Cursor cursor = database.rawQuery(sql, null);
        if (cursor.moveToFirst()) {
            do {
                habitacionesList.add(
                        new Habitacion(cursor.getInt(0),
                                cursor.getString(1))
                );
            } while(cursor.moveToNext());
        }
        cursor.close();
        return habitacionesList;
    }

    @Override
    public void agregar(Habitacion room) {
        SQLiteDatabase database = getWritableDatabase();
        ContentValues values = new ContentValues();
        values.put("nombre", room.getNombre());
        database.insert("habitaciones", null, values);
    }

    @Override
    public void actualizar(int id, Habitacion room) {
        SQLiteDatabase database = getWritableDatabase();
        String[] parametro = {String.valueOf(id)};
        ContentValues values = new ContentValues();
        values.put("nombre", room.getNombre());
        database.update("habitaciones", values, "_id=?", parametro);
    }

    @Override
    public void eliminar(int id) {
        SQLiteDatabase database = getWritableDatabase();
        String[] parametro = {String.valueOf(id)};
        database.delete("habitaciones", "_id=?", parametro);
    }
}