package queries;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import utils.CsvWriter;
import utils.QueriesPreprocessing;

import java.io.IOException;
import java.text.ParseException;

public class StartQueries {

    public static void main(String[] args) throws IOException, ParseException {

        System.out.println("----- start queries ----");

        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("Query 1")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");
        JavaRDD<String> rdd = QueriesPreprocessing.importParquet(spark).cache();

        /*
        JavaRDD<String> rdd = spark.read().csv("/home/martina/Documents/data/csv/yellow_tripdata_2022-01.csv")
                .toJavaRDD().map(
                row -> row.mkString(",")
        );
         */

        //Query1.query1Main(rdd);

        Query2.query2Main(rdd);
        spark.stop();

    }


}
