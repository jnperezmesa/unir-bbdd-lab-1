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
import java.util.Base64;

@Slf4j
public class ETLProcess {
    private static final String DATABASE = "fuel_db";

    public enum Margin {
        i, d, n
    }

    public enum Type {
        marítima, terrestre
    }

    public enum Fuels {

        gasolina95E5("gasolina 95 e5"), gasolina95E10("gasolina 95 e10"), gasoleoA("gasóleo a"),
        gasoleoB("gasóleo b"), gasolina95E5Pr("gasolina 95 e5 premium"), gasolina98E5("gasolina 98 e5"),
        gasolina98E10("gasolina 98 e5"), gasoleoPr("gasóleo premium"), gasoleoC("gasóleo c"),
        bioetanol("bioetanol"), biodiesel("biodiésel"), GLP("gases licuados del petróleo"),
        GNCom(" gas natural comprimido"), GNL("gas natural licuado"), hidrogeno("hidrógeno"),
        gasoleoMar("gasóleo de uso marítimo");
        private String nombre;

        Fuels(String nombre) {
            nombre = nombre;
        }

        String getNombre() {
            return nombre;
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
        private String nombre;

        Province(String nombre) {
            nombre = nombre;
        }

        String getNombre() {
            return nombre;
        }
    }

    //Gson https://www.youtube.com/watch?v=9oq7Y8n1t00
    public static void main(String[] args) {
        try (Connection connection = new MySqlConnector("localhost", DATABASE).getConnection()) {

            // funcion que llama a la base de datos

            GasStation[] gasStations = new GasStation[0];
            Gson gson = new Gson();
            String jsonRequest = gson.toJson(gasStations);

            String accessKey = System.getenv("BONSAI_ACCESS_KEY");
            String accessSecret = System.getenv("BONSAI_ACCESS_SECRET");
            // " + accessKey + ":" + accessSecret + "@
            String baseUriElastic = "https://gasolineras-4057692379.eu-west-1.bonsaisearch.net:443";

            String bonsaiAuth = Base64.getEncoder().encodeToString((accessKey + ":" + accessSecret).getBytes());

            HttpRequest testRequest = HttpRequest.newBuilder()
                    .uri(URI.create(baseUriElastic.concat("/stations/_mapping")))
                    .header("Authorization", "Basic " + bonsaiAuth)
                    .GET().build();
            /*
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(baseUriElastic.concat("/stations/_doc")))
                    .header("Content-Type", "aplication/json")
                    .POST(HttpRequest.BodyPublishers.ofString(jsonRequest)).build();
                    .header("Authorization", "Basic " + bonsaiAuth)
            */

            HttpClient client = HttpClient.newBuilder().build();

            HttpResponse<String> response = client.send(testRequest, HttpResponse.BodyHandlers.ofString());
            System.out.println(response.body());

        } catch (Exception e) {
            log.error("Error al tratar con la base de datos", e);
        }

    }

    @AllArgsConstructor
    @Getter
    private class GasStation {
        Type type;
        Margin margin;
        Province province;
        String municipality;
        String locality;
        String postalCode;
        String direction;
        double latitude;
        double longitude;
        Fuel[] fuels;
        String brand;
        String schedule;
    }

    @AllArgsConstructor
    @Getter
    private class Fuel {
        Fuels type;
        Double price;
        Date dateData;
    }
}
