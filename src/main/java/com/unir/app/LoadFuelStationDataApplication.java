package com.unir.app;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import com.unir.config.MySqlConnector;
import com.unir.model.FuelStation;
import com.unir.model.FuelType;
import com.unir.model.Province;
import com.unir.model.Municipality;
import com.unir.model.Town;
import com.unir.model.PostalCode;
import com.unir.model.Company;
import com.unir.model.Price;
import lombok.extern.slf4j.Slf4j;

import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
//import java.text.ParseException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.List;

@Slf4j
public class LoadFuelStationDataApplication {

    private static final String DATABASE = "fuel_db";

    public static void main(String[] args) {

        //Creamos conexion. No es necesario indicar puerto en host si usamos el default, 1521
        //Try-with-resources. Se cierra la conexión automáticamente al salir del bloque try
        try(Connection connection = new MySqlConnector("localhost", DATABASE).getConnection()) {

            log.info("Conexión establecida con la base de datos Oracle");

            // Add fuel types to the database
            Set<FuelType> fuelTypes = readFuelTypeData(
                    "fuel_type",
                    0
            );
            intakeFuelTypes(connection, fuelTypes);
            // Add provinces to the database
            Set<Province> provinces = readProvinceData(
                    "embarcacionesPrecios_es",
                    0
            );
            intakeProvinces(connection, provinces);
            // Add municipality to the database
            Set<Municipality> municipalities = readMunicipalityData(
                    "embarcacionesPrecios_es",
                    1
            );
            intakeMunicipalities(connection, municipalities);
            // Add town to the database
            Set<Town> towns = readTownData(
                    "embarcacionesPrecios_es",
                    2
            );
            intakeTowns(connection, towns);
            // Add postal code to the database
            //List<PostalCode> postalCodes = readPostalCodeData(
            //        "embarcacionesPrecios_es",
            //        3
            //);
            //intakePostalCodes(connection, postalCodes);
            // Add company to the database
            //List<Company> companies = readCompanyData(
            //        "embarcacionesPrecios_es",
            //        12
            //);
            //intakeCompanies(connection, companies);
            // Add fuel station to the database
            //List<FuelStation> fuelStations = readFuelStationData();
            //intakeFuelStations(connection, fuelStations);

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
    private static int getCompanyId(Connection connection, String company) throws SQLException, IOException {
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
     * Get id of the province form the database
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
     * Get id of the municipality form the database
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
     * Get id of the postal code form the database
     * @param connection - Connection to the database
     * @param postalCode - Name of the postalCode
     * @throws SQLException
     */
    private static int getPostalCodeId(Connection connection, int postalCode) throws SQLException {
        String query = "SELECT id FROM postal_code WHERE name = ?";
        PreparedStatement statement = connection.prepareStatement(query);
        statement.setInt(1, postalCode);
        ResultSet resultSet = statement.executeQuery();
        if (resultSet.next()) {
            return resultSet.getInt("id");
        } else {
            return -1;
        }
    }

    /**
     * Get id of the fuel type form the database
     * @param connection - Connection to the database
     * @param fuelType - Name of the fuel type
     * @throws SQLException
     */
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
     * Get id of the fuel station form the database
     * @param connection - Connection to the database
     * @param latitude - Latitude of the fuel station
     * @param longitude - Longitude of the fuel station
     * @throws SQLException
     */
    private static int getFuelStationId(Connection connection, float latitude, float longitude) throws SQLException {
        String query = "SELECT id FROM fuel_station WHERE latitude = ? AND longitude = ?";
        PreparedStatement statement = connection.prepareStatement(query);
        statement.setFloat(1, latitude);
        statement.setFloat(2, longitude);
        ResultSet resultSet = statement.executeQuery();
        if (resultSet.next()) {
            return resultSet.getInt("id");
        } else {
            return -1;
        }
    }

    /**
     * Get id of the price form the database
     * @param connection - Connection to the database
     * @param fuelStationId - Id of the fuel station
     * @param fuelTypeId - Id of the fuel type
     * @param createAt - Date of the price
     */
    private static boolean getPriceId(Connection connection, int fuelStationId, int fuelTypeId, Date createAt) throws SQLException {
        String query = "SELECT id FROM price WHERE fuel_station_id = ? AND fuel_type_id = ? AND date = ?";
        PreparedStatement statement = connection.prepareStatement(query);
        statement.setInt(1, fuelStationId);
        statement.setInt(2, fuelTypeId);
        statement.setDate(3, createAt);
        ResultSet resultSet = statement.executeQuery();
        if (resultSet.next()) {
            return false;
        } else {
            return true;
        }
    }

    /**
     * Get date of export from the first line and second column of CSV file
     * @return - Date of export
     */
    private static Date readDateData(String fileName) {

        // Try-with-resources. The reader closes automatically when exiting the try block.
        // CSVReader allows us to read the CSV file line by line.
        try (CSVReader reader = new CSVReader(new FileReader("csv/" + fileName + ".csv"))) {

            // Skip first line, which contains the names of the CSV columns
            String[] nextLine;

            // We read the file line by line
            while((nextLine = reader.readNext()) != null) {

                // Create date and return it
                SimpleDateFormat format = new SimpleDateFormat("dd/MM/yyyy");
                java.util.Date parsed = format.parse(nextLine[2]);
                Date date = new Date(parsed.getTime());
                return date;
            }
            return null;
        } catch (IOException e) {
            log.error("Error reading CSV file", e);
            throw new RuntimeException(e);
        } catch (CsvValidationException e) {
            throw new RuntimeException(e);
        } catch (java.text.ParseException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Read fuel types from CSV file
     * @return - List of fuel types
     */
    private static Set<FuelType> readFuelTypeData(String fileName, int column) {

        // Try-with-resources. The reader closes automatically when exiting the try block.
        // CSVReader allows us to read the CSV file line by line.
        try (CSVReader reader = new CSVReader(new FileReader("csv/" + fileName + ".csv"))) {

            // Create list of fuel types
            Set<FuelType> fuelTypes = new HashSet<>();

            // Skip first line, which contains the names of the CSV columns
            reader.skip(2);
            String[] nextLine;

            // We read the file line by line
            while((nextLine = reader.readNext()) != null) {
                // Create fuel type and add it to the list
                FuelType fuelType = new FuelType(
                        0,
                        nextLine[column].trim().toLowerCase()
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
     * Read provinces from CSV file
     * @return - List of provinces
     */
    private static Set<Province> readProvinceData(String fileName, int column) {

        // Try-with-resources. The reader closes automatically when exiting the try block.
        // CSVReader allows us to read the CSV file line by line.
        try (CSVReader reader = new CSVReader(new FileReader("csv/" + fileName + ".csv"))) {

            // Create list of provinces
            Set<Province> provinces = new HashSet<>();

            // Skip first line, which contains the names of the CSV columns
            reader.skip(2);
            String[] nextLine;

            // We read the file line by line
            while((nextLine = reader.readNext()) != null) {

                // Create province and add it to the list
                Province province = new Province(
                        0,
                        nextLine[column].trim().toLowerCase()
                );
                provinces.add(province);
            }
            return provinces;
        } catch (IOException e) {
            log.error("Error reading CSV file", e);
            throw new RuntimeException(e);
        } catch (CsvValidationException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Read municipalities from CSV file
     * @return - List of municipalities
     */
    private static Set<Municipality> readMunicipalityData(String fileName, int column) {

        // Try-with-resources. The reader closes automatically when exiting the try block.
        // CSVReader allows us to read the CSV file line by line.
        try (CSVReader reader = new CSVReader(new FileReader("csv/" + fileName + ".csv"))) {

            // Create list of municipalities
            Set<Municipality> municipalities = new HashSet<>();

            // Skip first line, which contains the names of the CSV columns
            reader.skip(2);
            String[] nextLine;

            // We read the file line by line
            while((nextLine = reader.readNext()) != null) {

                // Create municipality and add it to the list
                Municipality municipality = new Municipality(
                        0,
                        nextLine[column].trim().toLowerCase()
                );
                municipalities.add(municipality);
            }
            return municipalities;
        } catch (IOException e) {
            log.error("Error reading CSV file", e);
            throw new RuntimeException(e);
        } catch (CsvValidationException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Read towns from CSV file
     * @return - List of towns
     */
    private static Set<Town> readTownData(String fileName, int column) {

        // Try-with-resources. The reader closes automatically when exiting the try block.
        // CSVReader allows us to read the CSV file line by line.
        try (CSVReader reader = new CSVReader(new FileReader("csv/" + fileName + ".csv"))) {

            // Create list of towns
            Set<Town> towns = new HashSet<>();

            // Skip first line, which contains the names of the CSV columns
            reader.skip(2);
            String[] nextLine;

            // We read the file line by line
            while((nextLine = reader.readNext()) != null) {

                // Create town and add it to the list
                Town town = new Town(
                        0,
                        nextLine[column].trim().toLowerCase()
                );
                towns.add(town);
            }
            return towns;
        } catch (IOException e) {
            log.error("Error reading CSV file", e);
            throw new RuntimeException(e);
        } catch (CsvValidationException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Read postal codes from CSV file
     * @return - List of postal codes
     */
    private static Set<PostalCode> readPostalCodeData(String fileName, int column) {

        // Try-with-resources. The reader closes automatically when exiting the try block.
        // CSVReader allows us to read the CSV file line by line.
        try (CSVReader reader = new CSVReader(new FileReader("csv/" + fileName + ".csv"))) {

            // Create list of postal codes
            Set<PostalCode> postalCodes = new HashSet<>();

            // Skip first line, which contains the names of the CSV columns
            reader.skip(2);
            String[] nextLine;

            // We read the file line by line
            while((nextLine = reader.readNext()) != null) {

                // Create postal code and add it to the list
                PostalCode postalCode = new PostalCode(
                        0,
                        Integer.parseInt(nextLine[column].trim().toLowerCase())
                );
                postalCodes.add(postalCode);
            }
            return postalCodes;
        } catch (IOException e) {
            log.error("Error reading CSV file", e);
            throw new RuntimeException(e);
        } catch (CsvValidationException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Read companies from CSV file
     * @return - List of companies
     */
    private static Set<Company> readCompanyData(String fileName, int column) {

        // Try-with-resources. The reader closes automatically when exiting the try block.
        // CSVReader allows us to read the CSV file line by line.
        try (CSVReader reader = new CSVReader(new FileReader("csv/" + fileName + ".csv"))) {

            // Create list of companies
            Set<Company> companies = new HashSet<>();

            // Skip first line, which contains the names of the CSV columns
            reader.skip(2);
            String[] nextLine;

            // We read the file line by line
            while((nextLine = reader.readNext()) != null) {

                // Create company and add it to the list
                Company company = new Company(
                        0,
                        nextLine[column].trim().toLowerCase()
                );
                companies.add(company);
            }
            return companies;
        } catch (IOException e) {
            log.error("Error reading CSV file", e);
            throw new RuntimeException(e);
        } catch (CsvValidationException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Read fuel stations from CSV file
     * @return - List of fuel stations
     */
    private static List<FuelStation> readFuelStationData(
            Connection connection,
            String fileName,
            int colCompany,
            int colCommunity,
            int colProvince,
            int colCity,
            int colPostalCode,
            int colAddress,
            int colMargin,
            int colLatitude,
            int colLongitude,
            int colOpeningHours,
            boolean IsMaritime
    ) {

        try (CSVReader reader = new CSVReader(new FileReader("csv/" + fileName + ".csv"))) {

            List<FuelStation> fuelStations = new LinkedList<>();
            reader.skip(2);
            String[] nextLine;

            while((nextLine = reader.readNext()) != null) {
                int companyId = getCompanyId(connection, nextLine[colCompany]);
                int communityId = getMunicipalityId(connection, nextLine[colCommunity]);
                int provinceId = getProvinceId(connection, nextLine[colProvince]);
                int cityId = getTownId(connection, nextLine[colCity]);
                int postalCodeId = getPostalCodeId(connection, Integer.parseInt(nextLine[colPostalCode]));

                FuelStation fuelStation = new FuelStation(
                        0,
                        companyId,
                        communityId,
                        provinceId,
                        cityId,
                        postalCodeId,
                        nextLine[colAddress].trim().toLowerCase(),
                        nextLine[colMargin].trim().toLowerCase().charAt(0),
                        Float.parseFloat(nextLine[colLatitude].trim().toLowerCase()),
                        Float.parseFloat(nextLine[colLongitude].trim().toLowerCase()),
                        nextLine[colOpeningHours].trim().toLowerCase(),
                        IsMaritime
                );
                fuelStations.add(fuelStation);
            }
            return fuelStations;
        } catch (IOException | CsvValidationException | SQLException e) {
            log.error("Error reading CSV file", e);
            throw new RuntimeException(e);
        }
    }

    /** Read price data from CSV file
     * @return - List of prices
     */
    private static List<Price> readPriceData(
            Connection connection,
            String fileName,
            String fuelName,
            int colLatitude,
            int colLongitude,
            int colPrice,
            int colCreateDate
    ) {

            try (CSVReader reader = new CSVReader(new FileReader("csv/" + fileName + ".csv"))) {

                List<Price> prices = new LinkedList<>();
                reader.skip(2);
                String[] nextLine;

                while((nextLine = reader.readNext()) != null) {
                    int fuelStationId = getFuelStationId(connection, Float.parseFloat(nextLine[colLatitude]), Float.parseFloat(nextLine[colLongitude]));
                    int fuelTypeId = getFuelTypeId(connection, fuelName);
                    Date createAt;

                    if (!nextLine[colCreateDate].trim().isEmpty()) {
                        SimpleDateFormat format = new SimpleDateFormat("dd/MM/yyyy");
                        java.util.Date parsed = format.parse(nextLine[colCreateDate].trim().toLowerCase());
                        createAt = new Date(parsed.getTime());
                    } else {
                        createAt = readDateData(fileName);
                    }

                    Price price = new Price(
                            0,
                            fuelStationId,
                            fuelTypeId,
                            Float.parseFloat(nextLine[colPrice].trim().toLowerCase()),
                            createAt
                    );
                    prices.add(price);
                }
                return prices;
            } catch (IOException | CsvValidationException | SQLException e) {
                log.error("Error reading CSV file", e);
                throw new RuntimeException(e);
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
    }


    /**
     * Enter the fuel types in the database.
     * If it does not exist, it is inserted.
     * @param connection - Connection to the database.
     * @param fuelTypes - List of fuel types.
     */
    private static void intakeFuelTypes(Connection connection, Set<FuelType> fuelTypes) {

        // Create query
        String query = "INSERT INTO fuel_type (name) VALUES (?)";
        int lote = 100;
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

            // Commit and re-enable autocommit
            connection.commit();
            connection.setAutoCommit(true);

            log.info("Fuel types data entered into the database");
        } catch (SQLException e) {
            log.error("Error entering data into the database", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Enter the provinces in the database.
     * If it does not exist, it is inserted.
     * @param connection - Connection to the database.
     * @param provinces - List of provinces.
     */
    private static void intakeProvinces(Connection connection, Set<Province> provinces) {

        // Create query
        String query = "INSERT INTO province (name) VALUES (?)";
        int lote = 100;
        int contador = 0;

        // Try-with-resources. The statement is automatically closed when you exit the try block.
        try (PreparedStatement statement = connection.prepareStatement(query)) {

            // Disable autocommit
            connection.setAutoCommit(false);

            // We go through the list of provinces
            for (Province province : provinces) {

                // If the province does not exist, we insert it
                if (getProvinceId(connection, province.getName()) == -1) {
                    // We set the query parameters
                    statement.setString(1, province.getName());

                    // We execute the query
                    statement.addBatch();

                    if (++contador % lote == 0) {
                        statement.executeBatch();
                    }
                }
            }

            // We execute the batch if it has not been executed before
            statement.executeBatch();

            // Commit and re-enable autocommit
            connection.commit();
            connection.setAutoCommit(true);

            log.info("Provinces data entered into the database");
        } catch (SQLException e) {
            log.error("Error entering data into the database", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Enter the municipalities in the database.
     * If it does not exist, it is inserted.
     * @param connection - Connection to the database.
     * @param municipalities - List of municipalities.
     */
    private static void intakeMunicipalities(Connection connection, Set<Municipality> municipalities) {

        // Create query
        String query = "INSERT INTO municipality (name) VALUES (?)";
        int lote = 100;
        int contador = 0;

        // Try-with-resources. The statement is automatically closed when you exit the try block.
        try (PreparedStatement statement = connection.prepareStatement(query)) {

            // Disable autocommit
            connection.setAutoCommit(false);

            // We go through the list of municipalities
            for (Municipality municipality : municipalities) {

                // If the municipality does not exist, we insert it
                if (getMunicipalityId(connection, municipality.getName()) == -1) {
                    // We set the query parameters
                    statement.setString(1, municipality.getName());

                    // We execute the query
                    statement.addBatch();

                    if (++contador % lote == 0) {
                        statement.executeBatch();
                    }
                }
            }

            // We execute the batch if it has not been executed before
            statement.executeBatch();

            // Commit and re-enable autocommit
            connection.commit();
            connection.setAutoCommit(true);

            log.info("Municipalities data entered into the database");
        } catch (SQLException e) {
            log.error("Error entering data into the database", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Enter the towns in the database.
     * If it does not exist, it is inserted.
     * @param connection - Connection to the database.
     * @param towns - List of towns.
     */
    private static void intakeTowns(Connection connection, Set<Town> towns) {

        // Create query
        String query = "INSERT INTO town (name) VALUES (?)";
        int lote = 100;
        int contador = 0;

        // Try-with-resources. The statement is automatically closed when you exit the try block.
        try (PreparedStatement statement = connection.prepareStatement(query)) {

            // Disable autocommit
            connection.setAutoCommit(false);

            // We go through the list of towns
            for (Town town : towns) {

                // If the town does not exist, we insert it
                if (getTownId(connection, town.getName()) == -1) {
                    // We set the query parameters
                    statement.setString(1, town.getName());

                    // We execute the query
                    statement.addBatch();

                    if (++contador % lote == 0) {
                        statement.executeBatch();
                    }
                }
            }

            // We execute the batch if it has not been executed before
            statement.executeBatch();

            // Commit and re-enable autocommit
            connection.commit();
            connection.setAutoCommit(true);

            log.info("Towns data entered into the database");
        } catch (SQLException e) {
            log.error("Error entering data into the database", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Enter the postal codes in the database.
     * If it does not exist, it is inserted.
     * @param connection - Connection to the database.
     * @param postalCodes - List of postal codes.
     */
    private static void intakePostalCodes(Connection connection, Set<PostalCode> postalCodes) {

        // Create query
        String query = "INSERT INTO postal_code (name) VALUES (?)";
        int lote = 100;
        int contador = 0;

        // Try-with-resources. The statement is automatically closed when you exit the try block.
        try (PreparedStatement statement = connection.prepareStatement(query)) {

            // Disable autocommit
            connection.setAutoCommit(false);

            // We go through the list of postal codes
            for (PostalCode postalCode : postalCodes) {

                // If the postal code does not exist, we insert it
                if (getPostalCodeId(connection, postalCode.getCode()) == -1) {
                    // We set the query parameters
                    statement.setInt(1, postalCode.getCode());

                    // We execute the query
                    statement.addBatch();

                    if (++contador % lote == 0) {
                        statement.executeBatch();
                    }
                }
            }

            // We execute the batch if it has not been executed before
            statement.executeBatch();

            // Commit and re-enable autocommit
            connection.commit();
            connection.setAutoCommit(true);

            log.info("Postal codes data entered into the database");
        } catch (SQLException e) {
            log.error("Error entering data into the database", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Enter the companies in the database.
     * If it does not exist, it is inserted.
     * @param connection - Connection to the database.
     * @param companies - List of companies.
     */
    private static void intakeCompanies(Connection connection, Set<Company> companies) {

        // Create query
        String query = "INSERT INTO company (name) VALUES (?)";
        int lote = 100;
        int contador = 0;

        // Try-with-resources. The statement is automatically closed when you exit the try block.
        try (PreparedStatement statement = connection.prepareStatement(query)) {

            // Disable autocommit
            connection.setAutoCommit(false);

            // We go through the list of companies
            for (Company company : companies) {

                // If the company does not exist, we insert it
                if (getCompanyId(connection, company.getName()) == -1) {
                    // We set the query parameters
                    statement.setString(1, company.getName());

                    // We execute the query
                    statement.addBatch();

                    if (++contador % lote == 0) {
                        statement.executeBatch();
                    }
                }
            }

            // We execute the batch if it has not been executed before
            statement.executeBatch();

            // Commit and re-enable autocommit
            connection.commit();
            connection.setAutoCommit(true);

            log.info("Companies data entered into the database");
        } catch (SQLException | IOException e) {
            log.error("Error entering data into the database", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Enter the fuel stations in the database.
     * If it does not exist, it is inserted.
     * @param connection - Connection to the database.
     * @param fuelStations - List of fuel stations.
     */
    private static void intakeFuelStations(Connection connection, List<FuelStation> fuelStations) {

        // Creamos la query
        String query = "INSERT INTO fuel_station (company_id, community_id, province_id, city_id, postal_code_id, address, margin, latitude, longitude, opening_hours, is_maritime) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        int lote = 100;
        int contador = 0;

        // Try-with-resources. The statement is automatically closed when you exit the try block.
        try (PreparedStatement statement = connection.prepareStatement(query)) {

            // Disable autocommit
            connection.setAutoCommit(false);

            // We go through the list of fuel stations
            for (FuelStation fuelStation : fuelStations) {

                // If the fuel station does not exist, we insert it
                if (getFuelStationId(connection, fuelStation.getLatitude(), fuelStation.getLongitude()) == -1) {
                    // We set the query parameters
                    statement.setInt(1, fuelStation.getCompanyId());
                    statement.setInt(2, fuelStation.getCommunityId());
                    statement.setInt(3, fuelStation.getProvinceId());
                    statement.setInt(4, fuelStation.getCityId());
                    statement.setInt(5, fuelStation.getPostalCodeId());
                    statement.setString(6, fuelStation.getAddress());
                    statement.setString(7, String.valueOf(fuelStation.getMargin()));
                    statement.setFloat(8, fuelStation.getLatitude());
                    statement.setFloat(9, fuelStation.getLongitude());
                    statement.setString(10, fuelStation.getOpeningHours());
                    statement.setBoolean(11, fuelStation.getIsMaritime());

                    // We execute the query
                    statement.addBatch();

                    if (++contador % lote == 0) {
                        statement.executeBatch();
                    }
                }
            }

            // We execute the batch if it has not been executed before
            statement.executeBatch();

            // Commit and re-enable autocommit
            connection.commit();
            connection.setAutoCommit(true);

            log.info("Fuel stations data entered into the database");
        } catch (SQLException e) {
            log.error("Error entering data into the database", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Enter the prices in the database.
     * If it does not exist, it is inserted.
     * @param connection - Connection to the database.
     * @param prices - List of prices.
     */
    private static void intakePrices(Connection connection, List<Price> prices) {

        // Create query
        String query = "INSERT INTO price (fuel_station_id, fuel_type_id, price, date) VALUES (?, ?, ?, ?)";
        int lote = 100;
        int contador = 0;

        // Try-with-resources. The statement is automatically closed when you exit the try block.
        try (PreparedStatement statement = connection.prepareStatement(query)) {

            // Disable autocommit
            connection.setAutoCommit(false);

            // We go through the list of prices
            for (Price price : prices) {

                // If the price does not exist, we insert it
                if (getPriceId(connection, price.getFuelStationId(), price.getFuelTypeId(), price.getCreateAt())) {
                    // We set the query parameters
                    statement.setInt(1, price.getFuelStationId());
                    statement.setInt(2, price.getFuelTypeId());
                    statement.setFloat(3, price.getPrice());
                    statement.setDate(4, price.getCreateAt());

                    // We execute the query
                    statement.addBatch();

                    if (++contador % lote == 0) {
                        statement.executeBatch();
                    }
                }
            }

            // We execute the batch if it has not been executed before
            statement.executeBatch();

            // Commit and re-enable autocommit
            connection.commit();
            connection.setAutoCommit(true);

            log.info("Prices data entered into the database");
        } catch (SQLException e) {
            log.error("Error entering data into the database", e);
            throw new RuntimeException(e);
        }
    }
}
