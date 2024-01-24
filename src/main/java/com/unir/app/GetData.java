package com.unir.app;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

@Slf4j
public class GetData {

    /**
     *
     * @param connection - Connection to the database
     * @throws SQLException
     */
    public static int getAllStations(Connection connection) throws SQLException {
        String query = "SELECT id FROM town WHERE name =  LIMIT 1";
        PreparedStatement statement = connection.prepareStatement(query);
        ResultSet resultSet = statement.executeQuery();
        if (resultSet.next()) {
            System.out.println(resultSet.getInt("id"));
        } else {
            System.out.println("Finishing program.");
        }
    }
}
