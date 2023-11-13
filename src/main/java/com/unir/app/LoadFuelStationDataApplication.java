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
//import java.text.ParseException;
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
            List<FuelType> fuelTypes = readFuelTypeData();

            // Introducimos los datos en la base de datos
            intakeFuelTypes(connection, fuelTypes);

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
        String query = "SELECT id FROM company WHERE name = ?";
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
        String query = "SELECT id FROM province WHERE name = ?";
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
        String query = "SELECT id FROM municipality WHERE name = ?";
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
        String query = "SELECT id FROM town WHERE name = ?";
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
        String query = "SELECT id FROM postal_code WHERE name = ?";
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
        String query = "SELECT id FROM fuel_type WHERE name = ?";
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
     * Read fuel types from CSV file
     * @return - List of fuel types
     */
    private static List<FuelType> readFuelTypeData() {

        // Try-with-resources. The reader closes automatically when exiting the try block.
        // CSVReader allows us to read the CSV file line by line.
        try (CSVReader reader = new CSVReader(new FileReader("csv/fuel_type.csv"))) {

            // Create list of fuel types
            List<FuelType> fuelTypes = new LinkedList<>();

            // Skip first line, which contains the names of the CSV columns
            reader.skip(1);
            String[] nextLine;

            // We read the file line by line
            while((nextLine = reader.readNext()) != null) {

                // Creamos el empleado y lo añadimos a la lista
                FuelType fuelType = new FuelType(
                        0,
                        nextLine[0]
                );
                fuelTypes.add(fuelType);
            }
            return fuelTypes;
        } catch (IOException e) {
            log.error("Error reading CSV file", e);
            throw new RuntimeException(e);
        } catch (CsvValidationException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Enter the fuel types in the database.
     * If it does not exist, it is inserted.
     * @param connection - Connection to the database.
     * @param fuelTypes - List of fuel types.
     */
    private static void intakeFuelTypes(Connection connection, List<FuelType> fuelTypes) {

        // Creamos la query
        String query = "INSERT INTO fuel_type (name) VALUES (?)";
        int lote = 2;
        int contador = 0;

        // Try-with-resources. The statement is automatically closed when you exit the try block.
        try (PreparedStatement statement = connection.prepareStatement(query)) {

            // Disable autocommit
            connection.setAutoCommit(false);

            // We go through the list of fuels
            for (FuelType fuelType : fuelTypes) {

                // If the fuel does not exist, we insert it
                if (getFuelTypeId(connection, fuelType.getName()) == -1) {
                    // We set the query parameters
                    statement.setString(1, fuelType.getName());

                    // We execute the query
                    statement.addBatch();

                    if (++contador % lote == 0) {
                        statement.executeBatch();
                    }
                }
            }

            // We execute the batch if it has not been executed before
            statement.executeBatch();

            // Hacemos commit y volvemos a activar el autocommit
            connection.commit();
            connection.setAutoCommit(true);

            log.info("Fuel types data entered into the database");
        } catch (SQLException e) {
            log.error("Error entering data into the database", e);
            throw new RuntimeException(e);
        }
    }

}
