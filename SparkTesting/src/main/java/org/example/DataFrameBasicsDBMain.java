package org.example;

import org.example.application.SalesTransactionsDbManager;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.stream.Stream;


import static org.example.application.SalesTransactionsDbManager.DB_URL;
import static org.apache.spark.sql.functions.lit;



public class DataFrameBasicsDBMain {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataFrameBasicsDBMain.class);
    private static final String SPARK_FILES_FORMAT = "csv";
    public static final String PATH_RESOURCES = "src/main/resources/spark-data/electronic-card-transactions.csv";
    public static final String PATH_FOLDER_RESOURCES = "src/main/resources/spark-data/enriched_transactions";

    public static void main(String[] args) throws Exception {

        LOGGER.info("Application starting up");
        DataFrameBasicsDBMain app = new DataFrameBasicsDBMain();
        app.init();
        LOGGER.info("Application gracefully exiting...");
    }

    private void init() throws Exception {

        LOGGER.info("Deleting previous resources generated, if any...");
        deletePreviousFiles(PATH_FOLDER_RESOURCES);

        LOGGER.info("Bootstrapping DB Resources");
        //Start Embedded DB Server from within this application's process by connecting (Derby)
        SalesTransactionsDbManager salesTransactionsDbManager = new SalesTransactionsDbManager();
        salesTransactionsDbManager.startDB();

        //Create the Spark Session
        SparkSession session = SparkSession.builder()
                .appName("DataFrameBasicsDB")
                .master("local").getOrCreate();

        //Ingest data from CSV file into a DataFrame
        Dataset<Row> df = session.read()
                .format(SPARK_FILES_FORMAT)
                .option("header", "true")
                .load(PATH_RESOURCES);

        //Apply a transformation - Add a new Column to the dataframe for final transactions values
        Dataset<Row> results = df.withColumn("total_units_value",
                lit(df.col("Data_value").multiply(df.col("magnitude"))));


        //Persist to DB through Spark JDBC methods
        Properties prop = salesTransactionsDbManager.buildDBProperties();
        results.write().mode(SaveMode.Overwrite)
                .jdbc(DB_URL,
                        SalesTransactionsDbManager.TABLE_NAME, prop);

        //Print results in DB
        salesTransactionsDbManager.displayTableResults(5);

        //Create CSV Output file
        results.write().format(SPARK_FILES_FORMAT)
                .mode(SaveMode.Overwrite)
                .save("src/main/resources/spark-data/enriched_transactions");

        //Print first 5 rows to output
        results.show(5);
    }

    private void deletePreviousFiles(String resourcesPath) {
        try {
            Stream<Path> allContents = Files.list(Paths.get(resourcesPath));
            allContents.sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        } catch (Exception nfe) {
            LOGGER.info("No previous generated file found, skipping...");
        }

    }
}
