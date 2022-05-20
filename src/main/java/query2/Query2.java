package query2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import query1.Query1Preprocessing;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class Query2 {

    //private static String pathProva = "/home/martina/Documents/data/csv/provaQuery2.csv";
    private static String pathProva = "/home/martina/Documents/data/csv/provaQuery2Ordinato.csv";


    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("Query 2")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        //todo: preproc in cui ordino csv in base a prima colonna per data crescente e genero path

        JavaRDD<String> rdd = spark.read().csv(pathProva).toJavaRDD().map(
                row -> row.mkString(",")
        );


        JavaRDD<Tuple3<OffsetDateTime, Double, Double>> rdd2 = Query2Preprocessing.preprocData(rdd);

        /*
        for (Tuple3<OffsetDateTime, Double, Double> i : rdd2.collect()){
            System.out.println(i);
        }

         */

        // We create the pair word, 1 to count elements using
        // the number summarization pattern

        /* voglio che elementi nella stessa ora abbiano la stessa chiave
         ad esempio 2021-12-07T22:17:10+01:00 e 2021-12-07T22:39:12+01:00
         hanno stessa chiave 2021-12-07,22
         */

        JavaPairRDD<OffsetDateTime, Integer> pairs = rdd2.mapToPair(

                word -> {
                    OffsetDateTime tpep_pickup_datetime = word._1();
                    long epochMilli = tpep_pickup_datetime.toInstant().toEpochMilli();
                    Date date = new Date(epochMilli);
                    System.out.println("date == " + date);
                    return new Tuple2<>(word._1(), 1);
                });

        //rdd2.collect();

        for (Tuple2<OffsetDateTime, Integer> i : pairs.collect()){
            System.out.println(i);
        }
        System.out.println(" count == " + pairs.count());
        spark.stop();
    }


}
