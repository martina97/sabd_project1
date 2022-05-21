package query2;

import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
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

import static java.lang.Double.sum;

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

        System.out.println("\n\n ------- rdd2 ---------");
        for (Tuple3<OffsetDateTime, Double, Double> i : rdd2.collect()){
            System.out.println(i);
        }



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

        // CALCOLO Distribution of the number of trips distrNumbTripPerH
        JavaPairRDD<String, Integer> distrNumbTripPerH = CalculateDistribution(rdd2);


        //Calcolo average tip and its standard deviation
        JavaPairRDD<String, Double> avgAndStDevTip = CalculateAvgStDevTip(rdd2);
        //CsvWriter.writeQ2Results(counts2);


        System.out.println(" ------ outputQ2 -------");
        JavaPairRDD<String, Tuple2<Integer, Double>> outputQ2 = distrNumbTripPerH.join(avgAndStDevTip).sortByKey();
        for (Tuple2<String, Tuple2<Integer, Double>> i : outputQ2.collect()){
            System.out.println(i);
        }

        spark.stop();
    }

    private static JavaPairRDD<String, Double> CalculateAvgStDevTip(JavaRDD<Tuple3<OffsetDateTime, Double, Double>> rdd2) {
        System.out.println(" -------\n\n AVERAGE TIP -----------");

        /*
        questo rdd contiene: key = ora, value = (tip, 1)
        (2021-12-1 01,(2.0,1))
        (2021-12-1 01,(2.05,1))
        (2021-12-1 20,(2.06,1))
        (2021-12-1 20,(0.0,1))
         */
        JavaPairRDD<String,Tuple2<Double,Integer>> rddAvgTip = rdd2.mapToPair(
                word -> {
                    OffsetDateTime odt = word._1();
                    String key = getDateHour(odt);
                    Tuple2<Double,Integer> value = new Tuple2<>(word._3(),1);

                    return new Tuple2<>(key, value);
                });

        /*
        System.out.println(" -------\n\n rddAvgTip -----------");

        for (Tuple2<String, Tuple2<Double, Integer>> i : rddAvgTip.collect()){
            System.out.println(i);
        }

         */


        JavaPairRDD<String, Double> outputFin = rddAvgTip
                .reduceByKey((a, b) -> new Tuple2<>(a._1() + b._1(), a._2() + b._2())) // create a tuple (count, sum) for each key
                .mapToPair(k -> {
                    Tuple2<Double, Integer> value = k._2();
                    Double avg = value._1() / value._2();
                    return new Tuple2<>(k._1(), avg);  // calculate mean for each key
        });


        /*
        System.out.println(" \n------ outputFin -------");

        for (Tuple2<String, Double> i : outputFin.collect()){
            System.out.println(i);
        }

         */
        return outputFin;
    }

    private static JavaPairRDD<String, Integer> CalculateDistribution(JavaRDD<Tuple3<OffsetDateTime, Double, Double>> rdd) {

        //questo rdd contiene coppie (2021-11-30 hh, 1)
        return rdd.mapToPair(
                word -> {
                    //String tpep_pickup_datetime = word._1().toString();
                    OffsetDateTime odt = word._1();
                    String key = getDateHour(odt);

                    //System.out.println("key == " + key);

                    //System.out.println("dayOfYear == " + dayOfYear + "hour == " + hour);
                    //String tpep_pickup_datetimeHour = tpep_pickup_datetime.substring(0,13);

                    return new Tuple2<>(key, 1);
                })
                .reduceByKey((x, y) -> x+y); // We reduce the elements by key (i.e., word) and count
        /*
        for (Tuple2<String, Integer> i : pairs2.collect()){
            System.out.println(i);
        }

         */

    }

    private static String getDateHour(OffsetDateTime odt) {
        //String tpep_pickup_datetime = word._1().toString();
        int hourInt = odt.getHour();
        String hour = String.valueOf(hourInt);
        if (hourInt < 10) {
            hour = "0"+hourInt;
        }
        String key = odt.getYear() + "-" + odt.getMonthValue() + "-" + odt.getDayOfMonth() + " " + hour;
        return key;
    }

    private static PairFunction<Tuple2<String, Tuple2<Double, Integer>>,String,Double> getAverageByKey = (tuple) -> {
        Tuple2<Double, Integer> val = tuple._2;
        Double total = val._1;
        int count = val._2;
        Tuple2<String, Double> averagePair = new Tuple2<String, Double>(tuple._1, total / count);
        return averagePair;
    };
}
