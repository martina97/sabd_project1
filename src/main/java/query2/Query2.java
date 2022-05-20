package query2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import query1.Query1Preprocessing;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import utilities.CsvWriter;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class Query2 {

    //private static String pathProva = "/home/martina/Documents/data/csv/provaQuery2.csv";
    private static String pathProva = "/home/martina/Documents/data/csv/provaQuery2Ordinato.csv";
    private static String finalPath = "/home/martina/Documents/data/csv/output.csv";


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


        //todo: modo 1 con substring
        /*
        JavaPairRDD<String, Integer> pairs = rdd2.mapToPair(

                word -> {
                    String tpep_pickup_datetime = word._1().toString();
                    String tpep_pickup_datetimeHour = tpep_pickup_datetime.substring(0,13);

                    return new Tuple2<>(tpep_pickup_datetimeHour, 1);
                });

        //rdd2.collect();

        for (Tuple2<String, Integer> i : pairs.collect()){
            System.out.println(i);
        }
        System.out.println(" count == " + pairs.count());
        System.out.println("\n------\n\n");
        // We reduce the elements by key (i.e., word) and count
        JavaPairRDD<String, Integer> counts =
                pairs.reduceByKey((x, y) -> x+y);
        for (Tuple2<String, Integer> i : counts.collect()){
            System.out.println(i);
        }
        System.out.println(" count == " + counts.count());

         */

        // -------------------- PROVO ALTRO MODO SENZA SUBSTRING  --------
        JavaPairRDD<String, Integer> pairs2 = rdd2.mapToPair(

                word -> {
                    //String tpep_pickup_datetime = word._1().toString();
                    OffsetDateTime odt = word._1();
                    String key = odt.getYear() + "-" + odt.getMonthValue() + "-" + odt.getDayOfMonth() + " " + odt.getHour();
                    int hour = odt.getHour();
                    System.out.println("key == " + key);
                    
                    //System.out.println("dayOfYear == " + dayOfYear + "hour == " + hour);
                    //String tpep_pickup_datetimeHour = tpep_pickup_datetime.substring(0,13);

                    return new Tuple2<>(key, 1);
                });
        for (Tuple2<String, Integer> i : pairs2.collect()){
            System.out.println(i);
        }
        // We reduce the elements by key (i.e., word) and count
        JavaPairRDD<String, Integer> counts2 =
                pairs2.reduceByKey((x, y) -> x+y);
        for (Tuple2<String, Integer> i : counts2.collect()){
            System.out.println(i);
        }

        CsvWriter.writeQ2Results(counts2);
        spark.stop();
    }


}
