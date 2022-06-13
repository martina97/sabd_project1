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


import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Map;
import java.util.stream.Collectors;

import static utils.convertProva.getMaxOccurence2;

public class Query2 {


    public static void query2Main(JavaRDD<String> rdd) {

        // (tpep_pickup_datetime,PULocationID,payment_type,tip_amount)
        JavaRDD<Tuple4<LocalDateTime, Long, Double, Double>> rdd2 = QueriesPreprocessing.Query2Preprocessing(rdd).cache();

        Instant start = Instant.now();

        // ----- Calculate the percentage distribution of the number of trips with respect to the starting areas for each hour ------------
        JavaPairRDD<String, Iterable<Tuple2<Long, Double>>> distrNumbTripPerH = CalculateDistribution(rdd2);


        //--------   Calculate average tip and its standard deviation for each hour  --------
        JavaPairRDD<String, Tuple2<Double, Double>> avgAndStDevTip2 = CalculateAvgStDevTip(rdd2);



        // -------- Calculate the most popular payment method for each hour --------
        JavaPairRDD<String, Iterable<Tuple2<Integer, Double>>> mostPopularPayment = CalculateTopPayment(rdd2);



        JavaPairRDD<String, Tuple2<Tuple2<Iterable<Tuple2<Long, Double>>, Tuple2<Double, Double>>, Iterable<Tuple2<Integer, Double>>>> resultQ2 = distrNumbTripPerH
                .join(avgAndStDevTip2)
                .join(mostPopularPayment)
                .sortByKey();
        Instant end = Instant.now();

        System.out.println("Durata query2 : " + Duration.between(start,end).toMillis());


        System.out.println(" \n\n------ RESULT Q2 ------- ");
        for (Tuple2<String, Tuple2<Tuple2<Iterable<Tuple2<Long, Double>>, Tuple2<Double, Double>>, Iterable<Tuple2<Integer, Double>>>> s : resultQ2.take(10)){
            System.out.println(s);
        }

      CsvWriter.writeQuery2(resultQ2);




    }



    public static JavaPairRDD<String, Iterable<Tuple2<Long, Double>>> CalculateDistribution(JavaRDD<Tuple4<LocalDateTime, Long, Double, Double>> rdd) {
        //System.out.println(" --------------- CalculateDistribution ----------------");

        // (hour,(PULocationID,1))
        JavaPairRDD<String, Tuple2<Long, Integer>> rddLocation = rdd.mapToPair(
                word -> {
                    LocalDateTime odt = word._1();
                    String dateHour = getDateHour(odt);
                    Tuple2<Long, Integer> value = new Tuple2<>(word._2(), 1);
                    return new Tuple2<>(dateHour, value);
                });

        // key: hour, value: number of trips in hour
        Map<String, Long> map = rddLocation.countByKey();

        // (hour, [(PULocationID, percentage distribution of number of trips)])
        JavaPairRDD<String, Iterable<Tuple2<Long, Double>>> output = rddLocation
                .mapToPair(row -> new Tuple2<>(new Tuple2<>(row._1, row._2._1), row._2._2))
                .reduceByKey((x, y) -> x + y)
                .mapToPair(
                        row -> {
                            double perc = new BigDecimal(row._2 * 100 / (float) map.get(row._1._1)).setScale(2, RoundingMode.HALF_UP).doubleValue(); //arrotondo a 2 cifre decimali
                            return new Tuple2<>(row._1()._1(), new Tuple2<>(row._1()._2(), perc));
                        })
                .groupByKey();

        return output;
    }



    private static JavaPairRDD<String, Iterable<Tuple2<Integer, Double>>> CalculateTopPayment(JavaRDD<Tuple4<LocalDateTime, Long, Double, Double>> rdd) {

        // ((hour,payment_type),1)
        JavaPairRDD<Tuple2<String, Double>, Integer> rddAvgTip = rdd.mapToPair(
                row -> {
                    LocalDateTime odt = row._1();
                    String dateHour = getDateHour(odt);

                    Tuple2<String, Double> key = new Tuple2<>(dateHour, row._3());
                    return new Tuple2<>(key, 1);
                });


        JavaPairRDD<Tuple2<String, Double>, Integer> reduced = rddAvgTip.reduceByKey(
                (a, b) -> a + b
        ); // ((hour,payment_type), payment_type occurrences)

        //  ((hour,payment_type occurrences), payment_type)
        JavaPairRDD<Tuple2<String, Integer>, Double> boh = reduced.mapToPair(row ->
                new Tuple2<>(new Tuple2(row._1._1, row._2), row._1._2));


        JavaPairRDD<Tuple2<String, Integer>, Double> prova2 = boh.sortByKey(new Tuple2Comparator());

        // (hour, Iterable(Tuple2(occurrences, payment_type)
        JavaPairRDD<String, Iterable<Tuple2<Integer, Double>>> prova5 = prova2
                .mapToPair(x -> new Tuple2<>(x._1._1, new Tuple2<>(x._1._2, x._2))) // (hour, (occurrences, payment_type))
                .groupByKey();

       	return prova5;

    }



    private static JavaPairRDD<String, Tuple2<Double, Double>> CalculateAvgStDevTip(JavaRDD<Tuple4<LocalDateTime, Long, Double, Double>> rdd2) {

        // (hour, tip_amount)
        JavaPairRDD<String, Double> rddAvgTip = rdd2.mapToPair(
                word -> {
                    LocalDateTime odt = word._1();
                    String key = getDateHour(odt);
                    return new Tuple2<>(key, word._4());
                });


        // (hour, (mean tip_amount, stdev tip_amount)
        JavaPairRDD<String, Tuple2<Double, Double>> output = rddAvgTip
                .aggregateByKey(
                        new StatCounter(),
                        StatCounter::merge,
                        StatCounter::merge)
                .mapToPair(x -> new Tuple2<>(x._1(), new Tuple2<>( x._2().mean(),x._2().stdev())));

        return output;
    }


    public static String getDateHour(LocalDateTime odt) {
        int hourInt = odt.getHour();
        String hour = String.valueOf(hourInt);
        if (hourInt < 10) {
            hour = "0"+hourInt;
        }
        int dayInt =odt.getDayOfMonth();
        String day = String.valueOf(dayInt);
        if (dayInt < 10) {
            day = "0"+dayInt;
        }
        int monthInt =odt.getMonthValue();
        String month = String.valueOf(monthInt);
        if (monthInt < 10) {
            month = "0"+monthInt;
        }
        String key = odt.getYear() + "-" + month + "-" + day + "-" + hour;
        return key;
    }

}
