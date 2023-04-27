package com.nosqldriver.jdbc.http;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Enumeration;

@Deprecated
public class Main {
    public static class Test1 {
        @JsonProperty
        private String text = "foo";
    }


    public static void main(String[] args) throws SQLException, JsonProcessingException {

//        ObjectMapper om = new ObjectMapper();
//        String dbmd1 = om.writeValueAsString(new TransportableDatabaseMetaData(""));
//        String t1 = om.writeValueAsString(new Test1());
//        String eee = om.writeValueAsString(new TransportableException(new RuntimeException("rr")));



        for (Enumeration<Driver> e = DriverManager.getDrivers(); e.hasMoreElements();) {
            Driver d = e.nextElement();
            System.out.println("driver: " + d);
        }
//        Connection conn = DriverManager.getConnection("http://localhost:8080#jdbc:postgresql://localhost:5432/test_db", "alex", "alex");
        Connection conn = DriverManager.getConnection("http://localhost:8080", "alex", "alex");
        System.out.println("connection=" + conn);

        DatabaseMetaData dbmd = conn.getMetaData();
        System.out.println("dbmd=" + dbmd);
        Statement statement = conn.createStatement();
        System.out.println("statement=" + statement);

        ResultSet rs = statement.executeQuery("select id, text from test_identity");
        System.out.println("rs=" + rs);

        while (rs.next()) {
            System.out.printf("%d, %s\n", rs.getInt(1), rs.getString("text"));
        }


        Statement statement2 = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        System.out.println("statement2=" + statement2);
    }
}
