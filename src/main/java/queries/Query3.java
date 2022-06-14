package queries;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.util.StatCounter;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import utils.CsvWriter;
import utils.QueriesPreprocessing;
import utils.Tuple2Comparator;
import utils.convertProva;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;

public class Query3 {

    public static void query3Main(JavaRDD<String> rdd) {


        // (tpep_pickup_datetime, passenger_count, DOLocationID, fare_amount)
        JavaRDD<Tuple4<LocalDateTime, Double, Long, Double>> rddPreproc = QueriesPreprocessing.Query3Preprocessing(rdd);
        Instant start = Instant.now();

        JavaPairRDD<String, ArrayList<Tuple4<Long, Double, Double, Double>>> result = mostPopularZones(rddPreproc);

        Instant end = Instant.now();

        System.out.println("Durata query3 : " + Duration.between(start,end).toMillis());

        CsvWriter.writeQuery3(result);

    }



    private static JavaPairRDD<String, ArrayList<Tuple4<Long, Double, Double, Double>>> mostPopularZones(JavaRDD<Tuple4<LocalDateTime, Double, Long, Double>> rddPreproc) {
        // ((giorno, DOLocationID),StatCounter relativo a fare_amount)
        JavaPairRDD<Tuple2<String, Long>, StatCounter> rddFareAmount = rddPreproc
                .mapToPair(
                        word -> {
                            String date = covertDateToDayStr(word._1());
                            Tuple2<String, Long> key = new Tuple2<>(date, word._3());
                            Double value = (word._4());
                            return new Tuple2<>(key, value);
                        })
                .aggregateByKey(
                        new StatCounter(),
                         StatCounter::merge,
                        StatCounter::merge);

        // key : giorno, DOLocationID
        // value: (occorrenze della zona di destinazione per quel giorno, media fare_amount,
        //deviazione standard fare_amount)
        JavaPairRDD<Tuple2<String, Long>, Tuple3<Long, Double, Double>> meanStdevFareAmountRDD = rddFareAmount
                .mapToPair(x -> {
                    Tuple2<String, Long> key = new Tuple2<>(x._1._1, x._1._2);
                    Tuple3<Long, Double, Double> value = new Tuple3<>(x._2.count(), x._2.mean(), x._2.stdev());
                    return new Tuple2<>(key, value);
                });

        // key: (giorno, DOLocationID), value: passenger_count
        JavaPairRDD<Tuple2<String, Long>, Double> rddPassenger = rddPreproc.mapToPair(
                word -> {
                    String date = covertDateToDayStr(word._1());
                    Tuple2<String, Long> key = new Tuple2<>(date, word._3());   //chiave: (giorno, DOLocationID)
                    Double value = (word._2()); // valore: passenger_count
                    return new Tuple2<>(key, value);
                });


        // key: (giorno, DOLocationID), value: media passenger_count
        JavaPairRDD<Tuple2<String, Long>, Double> avgPax = rddPassenger
                .aggregateByKey(
                        new StatCounter(),
                        StatCounter::merge,
                        StatCounter::merge)
                .mapToPair(x -> new Tuple2<>(x._1, x._2.mean()));

        JavaPairRDD<Tuple2<String, Long>, Tuple2<Tuple3<Long, Double, Double>, Double>> rddJoin = meanStdevFareAmountRDD.join(avgPax);

        // key: (giorno, occorrenze DOLocationID) in modo da poter ordinare dall'occorrenza maggiore
        JavaPairRDD<Tuple2<String, Integer>, Tuple4<Long, Double, Double, Double>> rddJoinSorted = rddJoin.mapToPair(x ->
                {
                    Integer m = Math.toIntExact(x._2._1._1());
                    return new Tuple2<>(new Tuple2<>(x._1._1, m), new Tuple4<Long, Double, Double, Double>(x._1._2, x._2._1._2(), x._2._1._3(), x._2._2));
                }
        ).sortByKey(new Tuple2Comparator());


        JavaPairRDD<String, ArrayList<Tuple4<Long, Double, Double, Double>>> finale = rddJoinSorted
                .mapToPair(x -> new Tuple2<>(x._1._1, x._2))
                .groupByKey()
                .mapToPair(x -> {
                    Iterable<Tuple4<Long, Double, Double, Double>> top5Iterable = Iterables.limit(x._2, 5);
                    return new Tuple2<>(x._1, Lists.newArrayList(top5Iterable));
                }).sortByKey();


        /*
        System.out.println(" ---- finale ------ ");
        for (Tuple2<String, ArrayList<Tuple4>> s : finale.take(10)) {
            System.out.println(s);
        }

         */
        return finale;
    }
     public static String covertDateToDayStr(LocalDateTime localDateTime) {
        int day = localDateTime.getDayOfMonth();
        String dayStr = String.valueOf(day);
        if (day < 10) {
            dayStr = "0"+day;
        }

        int month =  localDateTime.getMonthValue();
        String monthStr = String.valueOf(month);

        if (month < 10) {
            monthStr = "0"+month;
        }
        return localDateTime.getYear() + "-" + monthStr + "-" + dayStr;
    }

}
