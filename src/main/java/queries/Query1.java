package queries;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.util.StatCounter;
import scala.Tuple2;
import scala.Tuple4;
import utils.CsvWriter;
import utils.QueriesPreprocessing;


import java.time.LocalDateTime;

public class Query1 {

    private static String finalPath = "/home/martina/Documents/data/csv/output.csv";
    //private static String finalPath = "prova_2021_12.csv";
    public static void query1Main(JavaRDD<String> rdd) {
    //public static void main(String[] args) {



        JavaRDD<Tuple4<LocalDateTime, Double, Double, Double>> rddPreproc = QueriesPreprocessing.Query1Preprocessing(rdd);
        //System.out.println("rddPreproc count === " + rddPreproc.count());

        JavaPairRDD<String, Double> resultRDD = computeResults(rddPreproc);
        CsvWriter.writeQuery1ResultsCSV(resultRDD);
        //CsvWriter.writeQuery1HDFS_CSV(resultRDD);

    }

    // PIU EFFICIENTE, CI METTE MENO
    // todo: vedere differenza di tempo tra questo metodo e l'altro
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
}
