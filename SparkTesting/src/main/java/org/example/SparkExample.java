package org.example;

import org.apache.spark.sql.*;
import org.apache.spark.sql.SparkSession;

public class SparkExample {

    private static final String APP_NAME = "DataFrameBasics";
    private static final String LOCAL_NODE_ID = "local";
    private static final String FORMAT = "csv";


    public static void main(String[] args) {

        SparkExample dataFrameBasicsMain = new SparkExample();
        dataFrameBasicsMain.init();
    }

    private static void init() {
//Create the Spark session on the localhost master node.


        Dataset<Row> df = null;
        SparkSession sparkSession =  SparkSession.builder().
         appName(APP_NAME).
         master(LOCAL_NODE_ID).
         getOrCreate();

// Read a CSV file with the header, and store it in a DataFrame.
         df = sparkSession.read().format(FORMAT)
                .option("header", "true")
                .load("/Users/batman/Downloads/fd_data_engineer_test/data/housing.csv");

//Show the first 15 rows.
        df.show(15);
    }
}