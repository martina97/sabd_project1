package queries;

import SQLqueries.SqlQuery1;
import SQLqueries.SqlQuery2;
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
        JavaRDD<String> rdd = QueriesPreprocessing.importParquet2(spark).cache();

        /*
        JavaRDD<String> rdd = spark.read().csv("/home/martina/Documents/data/csv/yellow_tripdata_2022-01.csv")
                .toJavaRDD().map(
                row -> row.mkString(",")
        );
         */

        //System.out.println("\n\n ------ Query 1 --------\n\n ");
        //Query1.query1Main(rdd);

        //System.out.println("\n\n ------ Query 2 --------\n\n ");
        //Query2.query2Main(rdd);

        //System.out.println("\n\n ------ Query 1 SQL--------\n\n ");
        //SqlQuery1.query1SQLMain(rdd, spark);

        System.out.println("\n\n ------ Query 2 SQL--------\n\n ");
        SqlQuery2.query2SQLMain(rdd, spark);
        spark.stop();

    }


}
