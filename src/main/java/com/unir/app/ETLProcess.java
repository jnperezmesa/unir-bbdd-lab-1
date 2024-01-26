package com.unir.app;

import com.google.gson.Gson;
import com.unir.config.MySqlConnector;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.*;
import java.sql.Date;
import java.util.*;

@Slf4j
public class ETLProcess {
    private static final String DATABASE = "fuel_db";

    public enum Margin {
        i, d, n
    }

    public enum Type {
        maritima, terrestre
    }

    public enum Fuels {

        gasolina_95_e5("gasolina 95 e5"), gasolina_95_e10("gasolina 95 e10"), gasoleo_a("gasóleo a"),
        gasoleo_b("gasóleo b"), gasolina95_e5_pr("gasolina 95 e5 premium"), gasolina_98_e5("gasolina 98 e5"),
        gasolina_98_e10("gasolina 98 e5"), gasoleo_pr("gasóleo premium"), gasoleo_c("gasóleo c"),
        bioetanol("bioetanol"), biodiesel("biodiésel"), glp("gases licuados del petróleo"),
        gncom("gas natural comprimido"), gnl("gas natural licuado"), hidrogeno("hidrógeno"),
        gasoleo_mar("gasóleo de uso marítimo");
        private String name;

        Fuels(String fuelName) {
            this.name = fuelName;
        }

        String getFuelName() {
            return name;
        }

        public static Fuels retrieveByFuelName(String fuelName) {
            for (Fuels fuel : Fuels.values()) {
                if (Objects.equals(fuel.getFuelName(), fuelName)) {
                    return fuel;
                }
            }
            return null;
        }
    }

    public enum Province {
        ALBACETE("albacete"), ALICANTE("alicante"), ALMERIA("almería"), ALAVA("araba/álava"),
        ASTURIAS("asturias"), AVILA("ávila"), BADAJOZ("badajoz"), BALEARES("balears (illes)"),
        BARCELONA("barcelona"), BIZKAIA("bizkaia"), BURGOS("burgos"), CACERES("cáceres"),
        CADIZ("cádiz"), CANTABRIA("cantabria"), CASTELLON("castellón"), CEUTA("ceuta"),
        REAL("ciudad real"), CORDOBA("córdoba"), CORUNA("coruña (a)"), CUENCA("cuenca"),
        GIPUZKOA("gipuzkoa"), GIRONA("girona"), GRANADA("granada"), GUADALAJARA("guadalajara"),
        HUELVA("huelva"), HUESCA("huesca"), JAEN("jaén"), LEON("león"), LLEIDA("lleida"),
        LUGO("lugo"), MADRID("madrid"), MALAGA("málaga"), MELILLA("melilla"),
        MURCIA("murcia"), NAVARRA("navarra"), OURENSE("ourense"), PALENCIA("palencia"),
        PALMAS("palmas (las)"), PONTEVEDRA("pontevedra"), RIOJA("rioja (la)"), SALAMANCA("salamanca"),
        TENERIFE("santa cruz de tenerife"), SEGOVIA("segovia"), SEVILLA("sevilla"), SORIA("soria"),
        TARRAGONA("tarragona"), TERUEL("teruel"), TOLEDO("toledo"), VALENCIA("valencia / valència"),
        VALLADOLID("valladolid"), ZAMORA("zamora"), ZARAGOZA("zaragoza");

        private String name;

        Province(String provinceName) {
            this.name = provinceName;
        }

        String getProvinceName() {
            return name;
        }

        public static Province retrieveProvinceByName(String provinceName) {
            for (Province province : Province.values()) {
                if (Objects.equals(province.getProvinceName(), provinceName)) {
                    return province;
                }
            }
            return null;
        }
    }


    @AllArgsConstructor
    @Getter
    public static class GasStation {
        Type type;
        Margin margin;
        Province province;
        String municipality;
        String locality;
        String postalCode;
        String direction;
        double latitude;
        double longitude;
        Map<Fuels, Double> fuels;
        String brand;
        String schedule;
        Date dateData;
    }

    //Gson https://www.youtube.com/watch?v=9oq7Y8n1t00
    public static void main(String[] args) {
        try (Connection connection = new MySqlConnector("localhost", DATABASE).getConnection()) {
            GasStation[] gasStations = getAllStations(connection);

            Gson gson = new Gson();
            String jsonRequest = gson.toJson(gasStations);
            System.out.println(jsonRequest);

/*
            String accessKey = System.getenv("BONSAI_ACCESS_KEY");
            String accessSecret = System.getenv("BONSAI_ACCESS_SECRET");
            // " + accessKey + ":" + accessSecret + "@
            String baseUriElastic = "https://gasolineras-4057692379.eu-west-1.bonsaisearch.net:443";

            String bonsaiAuth = Base64.getEncoder().encodeToString((accessKey + ":" + accessSecret).getBytes());

            HttpRequest testRequest = HttpRequest.newBuilder()
                    .uri(URI.create(baseUriElastic.concat("/stations/_mapping")))
                    .header("Authorization", "Basic " + bonsaiAuth)
                    .GET().build();

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(baseUriElastic.concat("/stations/_doc")))
                    .header("Content-Type", "aplication/json")
                    .POST(HttpRequest.BodyPublishers.ofString(jsonRequest)).build();
                    .header("Authorization", "Basic " + bonsaiAuth)


            HttpClient client = HttpClient.newBuilder().build();

            HttpResponse<String> response = client.send(testRequest, HttpResponse.BodyHandlers.ofString());
            System.out.println(response.body());
             */
        } catch (Exception e) {
            log.error("Error al tratar con la base de datos", e);
        }
    }

    /**
     * función que elimina los espacios en blanco de la cadena
     */

    private static Map<Fuels, Double> getFuels(Connection connection, int fuelStationId) throws SQLException {
        String query = "SELECT\n" +
                "    fuel_type.name AS 'fuel_type',\n" +
                "    price.price AS 'price',\n" +
                "    price.create_at AS 'create_at'\n" +
                "FROM\n" +
                "    price\n" +
                "JOIN\n" +
                "    fuel_type ON price.fuel_type_id = fuel_type.id\n" +
                "WHERE\n" +
                "    price.fuel_station_id = ?";
        PreparedStatement statement = connection.prepareStatement(query);
        statement.setInt(1, fuelStationId);
        ResultSet resultSet = statement.executeQuery();

        Map<Fuels, Double> fuels = new HashMap<Fuels, Double>();

        while (resultSet.next()) {
            fuels.put(Fuels.retrieveByFuelName(resultSet.getString("fuel_type")), resultSet.getDouble("price"));
        }
        return fuels;
    }

    /**
     *
     * @param connection - Connection to the database
     * @throws SQLException
     */
    private static GasStation[] getAllStations(Connection connection) throws SQLException {
        String query = "SELECT\n" +
            "    fuel_station.id AS 'id',\n" +
            "    fuel_station.is_maritime AS 'is_maritime',\n" +
            "    fuel_station.margin AS 'margin',\n" +
            "    province.name AS 'province',\n" +
            "    municipality.name AS 'municipality',\n" +
            "    town.name AS 'locality',\n" +
            "    postal_code.code AS 'postalCode',\n" +
            "    fuel_station.address AS 'direction',\n" +
            "    fuel_station.latitude AS 'latitude',\n" +
            "    fuel_station.longitude AS 'longitude',\n" +
            "    company.name AS 'company',\n" +
            "    (SELECT MIN(p.create_at)\n" +
            "     FROM price p\n" +
            "     WHERE p.fuel_station_id = fuel_station.id) AS 'registration_date'\n" +
            "FROM\n" +
            "    fuel_station\n" +
            "JOIN\n" +
            "    province ON fuel_station.province_id = province.id\n" +
            "JOIN\n" +
            "    municipality ON fuel_station.municipality_id = municipality.id\n" +
            "JOIN\n" +
            "    town ON fuel_station.town_id = town.id\n" +
            "JOIN\n" +
            "    postal_code ON fuel_station.postal_code_id = postal_code.id\n" +
            "JOIN\n" +
            "    company ON fuel_station.company_id = company.id";
        PreparedStatement statement = connection.prepareStatement(query);
        ResultSet resultSet = statement.executeQuery();

        ArrayList<GasStation> gasStations = new ArrayList<>();

        while (resultSet.next()) {
            int gasStationId = resultSet.getInt("id");
            Map<Fuels, Double> fuels = getFuels(connection, gasStationId);

            Boolean isMaritime = resultSet.getBoolean("is_maritime");
            Type type = isMaritime ? Type.maritima : Type.terrestre;

            Margin margin = Margin.valueOf(resultSet.getString("margin"));
            Province province = Province.retrieveProvinceByName(resultSet.getString("province"));

            GasStation gasStation = new GasStation(
                    type,
                    margin,
                    province,
                    resultSet.getString("municipality"),
                    resultSet.getString("locality"),
                    resultSet.getString("postalCode"),
                    resultSet.getString("direction"),
                    resultSet.getDouble("latitude"),
                    resultSet.getDouble("longitude"),
                    fuels,
                    resultSet.getString("company"),
                    resultSet.getString("registration_date"),
                    resultSet.getDate("registration_date")
            );
            System.out.println(gasStation.getPostalCode());
            gasStations.add(gasStation);
        }
        return gasStations.toArray(new GasStation[gasStations.size()]);
    }
}
