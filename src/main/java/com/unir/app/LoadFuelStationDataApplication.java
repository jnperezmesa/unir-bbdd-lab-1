package com.unir.app;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import com.unir.config.MySqlConnector;
import com.unir.model.FuelStation;
import com.unir.model.FuelType;
import lombok.extern.slf4j.Slf4j;

import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.LinkedList;
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

    /**
     * Get id of the company form the database
     * @param connection - Connection to the database
     * @param company - Name of the company
     * @throws SQLException
     */
    private static int getCompanyId(Connection connection, String company) throws SQLException {
        String query = "SELECT id FROM companies WHERE name = ?";
        PreparedStatement statement = connection.prepareStatement(query);
        statement.setString(1, company);
        ResultSet resultSet = statement.executeQuery();
        if (resultSet.next()) {
            return resultSet.getInt("id");
        } else {
            return -1;
        }
    }


    /**
     * Get id of the company form the database
     * @param connection - Connection to the database
     * @param province - Name of the province
     * @throws SQLException
     */
    private static int getProvinceId(Connection connection, String province) throws SQLException {
        String query = "SELECT id FROM provinces WHERE name = ?";
        PreparedStatement statement = connection.prepareStatement(query);
        statement.setString(1, province);
        ResultSet resultSet = statement.executeQuery();
        if (resultSet.next()) {
            return resultSet.getInt("id");
        } else {
            return -1;
        }
    }

    /**
     * Get id of the company form the database
     * @param connection - Connection to the database
     * @param municipality - Name of the municipality
     * @throws SQLException
     */
    private static int getMunicipalityId(Connection connection, String municipality) throws SQLException {
        String query = "SELECT id FROM municipalities WHERE name = ?";
        PreparedStatement statement = connection.prepareStatement(query);
        statement.setString(1, municipality);
        ResultSet resultSet = statement.executeQuery();
        if (resultSet.next()) {
            return resultSet.getInt("id");
        } else {
            return -1;
        }
    }

    /**
     * Get id of the town form the database
     * @param connection - Connection to the database
     * @param town - Name of the town
     * @throws SQLException
     */
    private static int getTownId(Connection connection, String town) throws SQLException {
        String query = "SELECT id FROM towns WHERE name = ?";
        PreparedStatement statement = connection.prepareStatement(query);
        statement.setString(1, town);
        ResultSet resultSet = statement.executeQuery();
        if (resultSet.next()) {
            return resultSet.getInt("id");
        } else {
            return -1;
        }
    }


    /**
     * Get id of the company form the database
     * @param connection - Connection to the database
     * @param postalCode - Name of the postalCode
     * @throws SQLException
     */
    private static int getPostalCodeId(Connection connection, String postalCode) throws SQLException {
        String query = "SELECT id FROM postal_codes WHERE name = ?";
        PreparedStatement statement = connection.prepareStatement(query);
        statement.setString(1, postalCode);
        ResultSet resultSet = statement.executeQuery();
        if (resultSet.next()) {
            return resultSet.getInt("id");
        } else {
            return -1;
        }
    }

    // Función que obtiene el id de la base de datos un tipo de combustible
    private static int getFuelTypeId(Connection connection, String fuelType) throws SQLException {
        String query = "SELECT id FROM fuel_types WHERE name = ?";
        PreparedStatement statement = connection.prepareStatement(query);
        statement.setString(1, fuelType);
        ResultSet resultSet = statement.executeQuery();
        if (resultSet.next()) {
            return resultSet.getInt("id");
        } else {
            return -1;
        }
    }

    /**
     * Lee los datos del fichero CSV y los devuelve en una lista de empleados.
     * El fichero CSV debe estar en la raíz del proyecto.
     *
     * @return - Lista de empleados
     */
    private static List<FuelType> readData() {

        // Try-with-resources. Se cierra el reader automáticamente al salir del bloque try
        // CSVReader nos permite leer el fichero CSV linea a linea
        try (CSVReader reader = new CSVReader(new FileReader("csv/fuel_type.csv"))) {

            // Creamos la lista de empleados y el formato de fecha
            List<FuelType> fuelTypes = new LinkedList<>();
            SimpleDateFormat format = new SimpleDateFormat("yyy-MM-dd");

            // Saltamos la primera linea, que contiene los nombres de las columnas del CSV
            reader.skip(1);
            String[] nextLine;

            // Leemos el fichero linea a linea
            while((nextLine = reader.readNext()) != null) {

                // Creamos el empleado y lo añadimos a la lista
                FuelType fuelType = new FuelType(
                        0,
                        nextLine[1],
                        nextLine[2],
                        nextLine[3],
                );
                fuelTypes.add(fuelType);
            }
            return fuelTypes;
        } catch (IOException e) {
            log.error("Error al leer el fichero CSV", e);
            throw new RuntimeException(e);
        } catch (CsvValidationException | ParseException e) {
            throw new RuntimeException(e);
        }
    }


}
