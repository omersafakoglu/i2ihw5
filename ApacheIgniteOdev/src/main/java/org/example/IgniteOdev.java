package org.example;

import org.apache.ignite.*;
import org.apache.ignite.Ignition;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;

public class IgniteOdev {
    public static void main(String[] args) {
        // JDBC connection parameters
        Ignite ignite = Ignition.start();
        String jdbcUrl = "jdbc:ignite:thin://127.0.0.1:10800/";
        String username = "odev";
        String password = "789";

        // SQL query to retrieve data from the "subscriber" table
        String sqlQuery = "SELECT * FROM subscriber";

        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             Statement stmt = conn.createStatement()) {

            // Execute the query
            ResultSet resultSet = stmt.executeQuery(sqlQuery);

            // Process the result set
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            while (resultSet.next()) {
                int subsc_id = resultSet.getInt("subsc_id");
                String subsc_name = resultSet.getString("subsc_name");
                String subsc_surname = resultSet.getString("subsc_surname");
                String msisdn = resultSet.getString("msisdn");
                int tariff_id = resultSet.getInt("tariff_id");
                Date start_date = resultSet.getDate("start_date");

                System.out.println("subsc_id: " + subsc_id
                        + ", subsc_name: " + subsc_name
                        + ", subsc_surname: " + subsc_surname
                        + ", msisdn: " + msisdn
                        + ", tariff_id: " + tariff_id
                        + ", start_date: " + dateFormat.format(start_date));
            }
            resultSet.close();
            stmt.close();
            conn.close();

            // Stop the Ignite cluster
            ignite.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

