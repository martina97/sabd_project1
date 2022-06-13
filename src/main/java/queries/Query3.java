package queries;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.util.StatCounter;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import utils.QueriesPreprocessing;
import utils.Tuple2Comparator;
import utils.convertProva;

import java.time.LocalDateTime;
import java.util.ArrayList;

public class Query3 {

    public static void query3Main(JavaRDD<String> rdd) {


        // (tpep_pickup_datetime, passenger_count, DOLocationID, fare_amount)
        JavaRDD<Tuple4<LocalDateTime, Double, Long, Double>> rddPreproc = QueriesPreprocessing.Query3Preprocessing(rdd);

        JavaPairRDD<String, ArrayList<Tuple4>> result = mostPopularZones(rddPreproc);
    }



    private static JavaPairRDD<String, ArrayList<Tuple4>> mostPopularZones(JavaRDD<Tuple4<LocalDateTime, Double, Long, Double>> rddPreproc) {
        // (giorno, DOLocationID)
        // (tpep_pickup_datetime, passenger_count, DOLocationID, fare_amount)

        JavaPairRDD<Tuple2<String, Long>, StatCounter> prova = rddPreproc
                .mapToPair(
                        word -> {

                            String date = convertProva.covertDateToDayStr(word._1());

                            Tuple2<String, Long> key = new Tuple2<>(date, word._3());
                            Double value = (word._4());
                            return new Tuple2<>(key, value);
                        })
                .aggregateByKey(
                        new StatCounter(),
                         StatCounter::merge,
                        StatCounter::merge);

        // (giorno, (occorrenze, media fare amount, stdev fare amount)
        JavaPairRDD<Tuple2<String, Long>, Tuple3<Long, Double, Double>> provaa = prova
                .mapToPair(x -> {
                    // chiave: (giorno, occorrenze)
                    Tuple2<String, Long> key = new Tuple2<>(x._1._1, x._1._2);
                    // valore: (zona, media fare amount, stdev fare amount)x._2.count()
                    Tuple3<Long, Double, Double> value = new Tuple3<>(x._2.count(), x._2.mean(), x._2.stdev());
                    return new Tuple2<>(key, value);
                });

        System.out.println(" ----------------------- NUM MEDIO PASSEGGERI ---------------------- ");
        JavaPairRDD<Tuple2<String, Long>, Double> rddPassenger = rddPreproc.mapToPair(
                word -> {
                    String date = convertProva.covertDateToDayStr(word._1());
                    Tuple2<String, Long> key = new Tuple2<>(date, word._3());   //chiave: (giorno, DOLocationID)
                    Double value = (word._2()); // valore: passenger_count
                    return new Tuple2<>(key, value);
                });


        JavaPairRDD<Tuple2<String, Long>, Double> avgPax = rddPassenger
                .aggregateByKey(
                        new StatCounter(),
                        StatCounter::merge,
                        StatCounter::merge)
                .mapToPair(x -> new Tuple2<>(x._1, x._2.mean()));

        // ((2021-12-1,236),((2,21.25,3.25),1.0)) : ((giorno,zona),((occorrenze,media fare_amount,std fare_amount),num medio passeggeri))
        JavaPairRDD<Tuple2<String, Long>, Tuple2<Tuple3<Long, Double, Double>, Double>> rddJoin = provaa.join(avgPax);

        // ora come chiave metto (giorno, occorrenze) in modo da poter ordinare dall'occorrenza maggiore
        JavaPairRDD<Tuple2<String, Integer>, Tuple4> rddJoinSorted = rddJoin.mapToPair(x ->
                {
                    Integer m = Math.toIntExact(x._2._1._1());
                    return new Tuple2<>(new Tuple2<>(x._1._1, m), new Tuple4(x._1._2, x._2._1._2(), x._2._1._3(), x._2._2));
                }
        ).sortByKey(new Tuple2Comparator());


        JavaPairRDD<String, ArrayList<Tuple4>> finale = rddJoinSorted
                .mapToPair(x -> new Tuple2<>(x._1._1, x._2))
                .groupByKey()
                .mapToPair(x -> {
                    Iterable<Tuple4> top5Iterable = Iterables.limit(x._2, 5);
                    return new Tuple2<>(x._1, Lists.newArrayList(top5Iterable));
                }).sortByKey();


        System.out.println(" ---- finale ------ ");
        for (Tuple2<String, ArrayList<Tuple4>> s : finale.take(10)) {
            System.out.println(s);
        }
        return finale;
    }
}
