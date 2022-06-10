package queries;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.util.StatCounter;
import scala.Tuple2;
import scala.Tuple5;
import utils.QueriesPreprocessing;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;

public class Query1 {

    private static String finalPath = "/home/martina/Documents/data/csv/output.csv";
    //private static String finalPath = "prova_2021_12.csv";
    public static void query1Main(JavaRDD<String> rdd) {
    //public static void main(String[] args) {

        JavaRDD<Tuple5<LocalDateTime, Double, Double, Double, Double>> rddPreproc = QueriesPreprocessing.Query1Preprocessing(rdd);
        //System.out.println("rddPreproc count === " + rddPreproc.count());
	Instant start = Instant.now();
        // resultRDD : (2021-12, (mean, count))
        JavaPairRDD<String, Tuple2<Double, Long>> resultRDD = computeResults(rddPreproc);
	Instant end = Instant.now();
	System.out.println("Durata query1 : " + Duration.between(start,end).toMillis());
	
        for (Tuple2<String, Tuple2<Double, Long>> s : resultRDD.collect()) {
            System.out.println(s);
        }
	
        //CsvWriter.writeQuery1ResultsCSV(resultRDD);
        CsvWriter.writeQuery1HDFS_CSV(resultRDD);

    }

        //  VECCHIO PRIMA DI TRACCIA UFFICIALE
    // PIU EFFICIENTE, CI METTE MENO
    // todo: vedere differenza di tempo tra questo metodo e l'altro
    /*
    private static JavaPairRDD<String, Double> computeResults(JavaRDD<Tuple4<LocalDateTime,Double, Double, Double>> rdd) {
        // voglio pair RDD con key = 2021-12, value = tip/(total amount - toll amount)

        JavaPairRDD<String, Double> rddAvgTip = rdd.mapToPair(
                word -> {
                    LocalDateTime odt = word._1();
                    String key = odt.getYear() + "-" + odt.getMonthValue();
                    Double value = word._2() / (word._4()- word._3());
                    //Tuple2<Double,Integer> value = new Tuple2<>(word._3(),1);

                    return new Tuple2<>(key, value);
                });
        JavaPairRDD<String, Double> output = rddAvgTip
                .filter(x-> !(Double.isNaN(x._2()))) //rimuovo NaN generati da tip/(total amount - toll amount) (infatti 0.0/0.0 = NaN)
                .aggregateByKey(
                        new StatCounter(),
                        StatCounter::merge,
                        StatCounter::merge)
                .mapToPair(x -> new Tuple2<>(x._1(),  x._2().mean()))
                .sortByKey();

        return output;
    }

     */


    // PIU EFFICIENTE, CI METTE MENO
    // todo: vedere differenza di tempo tra questo metodo e l'altro
    private static JavaPairRDD<String, Tuple2<Double, Long>> computeResults(JavaRDD<Tuple5<LocalDateTime, Double, Double, Double, Double>> rdd) {
        // voglio pair RDD con key = 2021-12, value = tip/(total amount - toll amount)

        JavaPairRDD<String, Double> rddAvgTip = rdd.mapToPair(
                word -> {
                    LocalDateTime odt = word._1();
                    String key = odt.getYear() + "-" + odt.getMonthValue();
                    Double value = word._3() / (word._5()- word._4());
                    //Tuple2<Double,Integer> value = new Tuple2<>(word._3(),1);

                    return new Tuple2<>(key, value);
                });

        /*
        rddAvgTip --> (key, value) = (mese, tip/tot-toll)
        facendo aggregateByKey prendo tutti i valori per quella chiave e ci applico diverse statistiche
         */


        /*
        JavaPairRDD<String, StatCounter> provaStatCount = rddAvgTip
                .filter(x-> !(Double.isNaN(x._2()))) //rimuovo NaN generati da tip/(total amount - toll amount) (infatti 0.0/0.0 = NaN)
                .aggregateByKey(
                        new StatCounter(),
                        StatCounter::merge,
                        StatCounter::merge);

        provaStatCount ==========  (2021-12,(count: 11, mean: 0,159004, stdev: 0,044313, max: 0,228669, min: 0,067568))
        quindi quando poi faccio .mapToPair(x -> new Tuple2<>(x._1(),  x._2().mean())), x._1() è la chiave , quindi la data,
        mentre x._2() sono (count: 11, mean: 0,159004, stdev: 0,044313, max: 0,228669, min: 0,067568)

         System.out.println("\n\n ----- StatCounter ------  ");
        for (Tuple2<String, StatCounter> s : provaStatCount.collect()) {
            System.out.println(s);
        }
        */



        JavaPairRDD<String, Tuple2<Double, Long>> output = rddAvgTip
                .filter(x -> !(Double.isNaN(x._2()))) //rimuovo NaN generati da tip/(total amount - toll amount) (infatti 0.0/0.0 = NaN)
                .aggregateByKey(
                        new StatCounter(),
                        StatCounter::merge,
                        StatCounter::merge)
                .mapToPair(x -> new Tuple2<>(x._1(), new Tuple2<>(x._2().mean(), x._2().count())))
                .sortByKey();
        return output;
    }
}
