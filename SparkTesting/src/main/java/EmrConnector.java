import org.apache.spark.sql.SparkSession;
public class EmrConnector {

        //accepts appname and endpoint
        //"hdfs://my-emr-cluster:8020"
        public SparkSession connectEMR (String APP_NAME,String END_POINT ) {
            // Create a SparkSession
            SparkSession spark = SparkSession.builder().appName(APP_NAME).getOrCreate();

            // Connect to the EMR cluster
            spark.conf().set("spark.hadoop.fs.defaultFS", END_POINT);


            // Run a Spark job
            spark.sql("SELECT * FROM my_table");
            return spark;
        }
    }

