package org.example.application;


  import org.slf4j.Logger;
  import org.slf4j.LoggerFactory;
  import java.sql.*;
  import java.util.Properties;
  import java.sql.DriverManager;
  import
          java.sql.Connection;

public class SalesTransactionsDbManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(SalesTransactionsDbManager.class);

    public static final String TABLE_NAME = "INIT";
    public static final String DB_URL = "jdbc:derby:firstdb;create=true;user=app;password=derby";
    public static final String EMBEDDED_DRIVER_STRING = "org.apache.derby.jdbc.EmbeddedDriver";

    public void startDB() throws SQLException {
        Connection conn = DriverManager.getConnection(DB_URL);
        createTable(conn);
    }

    public Properties buildDBProperties() {
        Properties props = new Properties();
        props.setProperty("driver", "org.apache.derby.jdbc.EmbeddedDriver");
        props.setProperty("user", "app");
        props.setProperty("password", "derby");
        return props;
    }

    public void displayTableResults(int numRecords) throws Exception {
        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;

        try {
            int printed = 0;
            Class.forName(EMBEDDED_DRIVER_STRING).newInstance();
            conn = DriverManager
                    .getConnection(DB_URL);
            statement = conn
                    .prepareStatement("SELECT * FROM " + TABLE_NAME);

            resultSet = statement.executeQuery();
            ResultSetMetaData rsmd = resultSet.getMetaData();
            int columnsNumber = rsmd.getColumnCount();

            while (resultSet.next() && printed < numRecords) {
                LOGGER.info("======= RECORD: " + printed + " ===========");
                for (int i = 1; i <= columnsNumber; i++) {
                    if (i > 1) System.out.print(",  ");
                    String columnValue = resultSet.getString(i);
                    LOGGER.info("COLUMN: " + rsmd.getColumnName(i) +  " - VALUE:  " + columnValue);
                }
                LOGGER.info("================================");
                printed++;
            }

        } catch (Exception e) {
            throw e;
        } finally {
            close(conn, statement, resultSet);
        }
    }

    private void close(Connection conn, PreparedStatement statement, ResultSet resultSet) {
        try {
            if (resultSet != null) {
                resultSet.close();
            }
            if (statement != null) {
                statement.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (Exception e) {
        }
    }

    //Table in creation statement acts as a placeholder,
    // if columns differ spark will delete the table and create one according to the DataFrame structure
    private void createTable(Connection connection) throws SQLException {

        try (Statement stmt = connection.createStatement()) {
            // create placeholder empty table
            String query = "CREATE TABLE INIT" + " (id integer)";
            stmt.executeUpdate(query);
        } catch (SQLException e) {
            //Log exception and rethrow
            LOGGER.error("Error while creating the TABLE in DB: " + e.getMessage());
        }
    }
}

