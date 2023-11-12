package com.unir.app;

import com.unir.config.MySqlConnector;
import com.unir.model.FuelStation;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.util.List;

@Slf4j
public class LoadFuelStationDataApplication {

    private static final String DATABASE = "fuel_db";

    public static void main(String[] args) {

        //Creamos conexion. No es necesario indicar puerto en host si usamos el default, 1521
        //Try-with-resources. Se cierra la conexión automáticamente al salir del bloque try
        try(Connection connection = new MySqlConnector("localhost", DATABASE).getConnection()) {

            log.info("Conexión establecida con la base de datos Oracle");

            // Leemos los datos del fichero CSV
            List<FuelStation> fuelStations = readData();

        } catch (Exception e) {
            log.error("Error al tratar con la base de datos", e);
        }
    }


}
