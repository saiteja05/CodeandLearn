package org.example.application;

import java.sql.*;

public class PostgreSQLJDBC {

    static String DRIVER = "org.postgresql.Driver";
    public static void main(String args[]) throws SQLException {
        Connection c = null;
        try {
          Class.forName(DRIVER).newInstance();
            c = DriverManager
                    .getConnection("jdbc:postgresql://localhost:5432/postgres","batman","password");
            System.out.println("Opened database successfully");
            System.out.println("Number of columns in Housing data table is : ");
            Statement s = c.createStatement();
           ResultSet rs = s.executeQuery("select * from housingdata;");
            ResultSetMetaData m = rs.getMetaData();
         System.out.println(m.getColumnCount());


        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(e.getClass().getName()+": "+e.getMessage());
            System.exit(0);
        }
        finally {
        c.close();
        System.out.println("Connection Closed");
        }

    }
}