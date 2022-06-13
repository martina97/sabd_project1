package queries;

import SQLqueries.SqlQuery1;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import utils.CsvWriter;
import utils.QueriesPreprocessing;

import java.io.IOException;
import java.text.ParseException;

public class StartQueries {

    public static void main(String[] args) throws InterruptedException {

        System.out.println("----- start queries ----");

        SparkSession spark = SparkSession.builder()
                .master("spark://spark:7077")
                .appName("sabd_project1")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");
        JavaRDD<String> rdd = QueriesPreprocessing.importParquet(spark).cache();

        /*
        JavaRDD<String> rdd = spark.read().csv("/media/sf_proj_condiviso/query2/Cartel2.csv")
                .toJavaRDD().map(
                row -> row.mkString(",")
        );

         */


       System.out.println("\n\n ------ Query 1 --------\n\n ");
       Query1.query1Main(rdd);

       // System.out.println("\n\n ------ Query 2 --------\n\n ");
        //Query2.query2Main(rdd);

        System.out.println("\n\n ------ Query 3 --------\n\n ");
        Query3.query3Main(rdd);

        //System.out.println("\n\n ------ Query 1 SQL--------\n\n ");
        //SqlQuery1.query1SQLMain(rdd, spark);

        //System.out.println("\n\n ------ Query 2 SQL--------\n\n ");
        //SqlQuery2.query2SQLMain(rdd, spark);
       //Thread.sleep(86400000); 
       spark.stop();

    }


}
