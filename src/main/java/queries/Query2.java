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



        JavaPairRDD<String, Iterable<Tuple2<Long, Double>>> distrNumbTripPerH = CalculateDistribution(rdd2);
        //todo: ordino la lista dei valori quando scrivo il csv !!!!!
        //todo: posso creare una mappa in cui la chiave e' la zona e id e' percentuale?

        //--------   Calcolo average tip and its standard deviation --------
        JavaPairRDD<String, Tuple2<Double, Double>> avgAndStDevTip2 = CalculateAvgStDevTip2(rdd2);




        // -------- Calcolo the most popular payment method --------
        JavaPairRDD<String, Iterable<Tuple2<Integer, Double>>> mostPopularPayment = CalculateTopPaymentComparator(rdd2);



        JavaPairRDD<String, Tuple2<Tuple2<Iterable<Tuple2<Long, Double>>, Tuple2<Double, Double>>, Iterable<Tuple2<Integer, Double>>>> resultQ2 = distrNumbTripPerH
                .join(avgAndStDevTip2)
                .join(mostPopularPayment)
                .sortByKey();

        System.out.println(" \n\n------ RESULT Q2 ------- ");
        for (Tuple2<String, Tuple2<Tuple2<Iterable<Tuple2<Long, Double>>, Tuple2<Double, Double>>, Iterable<Tuple2<Integer, Double>>>> s : resultQ2.take(10)){
            System.out.println(s);
        }

      CsvWriter.writeQuery2(resultQ2);




    }



    public static JavaPairRDD<String, Iterable<Tuple2<Long, Double>>> CalculateDistribution(JavaRDD<Tuple4<LocalDateTime, Long, Double, Double>> rdd) {
        System.out.println(" --------------- CalculateDistribution ----------------");

        // (hour,(PULocationID,1))
        JavaPairRDD<String, Tuple2<Long, Integer>> prova = rdd.mapToPair(
                word -> {
                    LocalDateTime odt = word._1();
                    String dateHour = getDateHour(odt);
                    Tuple2<Long, Integer> value = new Tuple2<>(word._2(), 1);
                    return new Tuple2<>(dateHour, value);
                });

        // key: hour, value: number of trips in hour
        Map<String, Long> map = prova.countByKey();

        // (hour, [(PULocationID, percentage distribution of number of trips)])
        JavaPairRDD<String, Iterable<Tuple2<Long, Double>>> prova2 = prova
                .mapToPair(row -> new Tuple2<>(new Tuple2<>(row._1, row._2._1), row._2._2))
                .reduceByKey((x, y) -> x + y)
                .mapToPair(
                        row -> {
                            //long perc =Math.round(row._2 * 100/ (float)map.get(row._1._1));
                            double perc = new BigDecimal(row._2 * 100 / (float) map.get(row._1._1)).setScale(2, RoundingMode.HALF_UP).doubleValue(); //arrotondo a 2 cifre decimali

                            //Long occ = map.get(row._1._1);
                            // System.out.println(" occ === " + occ);
                            return new Tuple2<>(row._1()._1(), new Tuple2<>(row._1()._2(), perc));
                        }
                ).
                groupByKey();
        /*
        for (Tuple2<String, Iterable<Tuple2<Long, Double>>> s: prova5.collect()) {
            System.out.println(s);
        }

         */


        return prova2;
    }



    private static JavaPairRDD<String, Iterable<Tuple2<Integer, Double>>> CalculateTopPaymentComparator(JavaRDD<Tuple4<LocalDateTime, Long, Double, Double>> rdd) {

        // ((ora,pagamento),1) --> ((1,1.0),1)
        JavaPairRDD<Tuple2<String, Double>, Integer> rddAvgTip = rdd.mapToPair(
                row -> {
                    LocalDateTime odt = row._1();
                    String dateHour = getDateHour(odt);

                    Tuple2<String, Double> key = new Tuple2<>(dateHour, row._3());
                    return new Tuple2<>(key, 1);
                });
        /*
        for (Tuple2<Tuple2<Integer, Double>, Integer> s : rddAvgTip.take(10)) {
            System.out.println(s);
        }

         */

        // (ora, pagamento), numero occorrenze) --> ((1,1.0),45)
        //System.out.println( " ----- reduced ------ ");

        JavaPairRDD<Tuple2<String, Double>, Integer> reduced = rddAvgTip.reduceByKey(
                (a, b) -> a + b
        );
        /*
        for (Tuple2<Tuple2<Integer, Double>, Integer> s : reduced.take(10)) {
            System.out.println(s);
        }

 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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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
        }

        */


        //System.out.println( " ----- boh ------ ");
        // (ora, numero occorrenze), pagamento --> ((1,45), 1.0)
        JavaPairRDD<Tuple2<String, Integer>, Double> boh = reduced.mapToPair(row ->
                new Tuple2<>(new Tuple2(row._1._1, row._2), row._1._2));
/*
        for (Tuple2<Tuple2<String, Integer>, Double> s : boh.collect()) {
            System.out.println(s);
        }

 */

        JavaPairRDD<Tuple2<String, Integer>, Double> prova2 = boh.sortByKey(new Tuple2Comparator());
/*
        System.out.println(" ---- prova2 -----");
        for (Tuple2<Tuple2<String, Integer>, Double> s : prova2.collect())
        {
            System.out.println(s);
        }

 */
        JavaPairRDD<String, Iterable<Tuple2<Integer, Double>>> prova5 = prova2
                .mapToPair(x -> new Tuple2<>(x._1._1, new Tuple2<>(x._1._2, x._2)))
                .groupByKey();

        System.out.println(" ---- prova5 -----");
        for (Tuple2<String, Iterable<Tuple2<Integer, Double>>> s : prova5.take(10))
        {
            System.out.println(s);
        }

/*
        JavaPairRDD<String, Tuple2<Integer, Double>> prova6 = prova5.mapToPair(x -> {
            Iterable<Tuple2<Integer, Double>> iterable = x._2;
            System.out.println("iterable == " + iterable);
            Tuple2<Integer, Double> firstElem = Iterables.get(iterable, 0);
            System.out.println("firstElem ==  " + firstElem);

            System.out.println(" ----- ");

            return new Tuple2<>(x._1, firstElem);
        });
        System.out.println(" ---- prova6 -----");
        for (Tuple2<String, Tuple2<Integer, Double>> s : prova6.sortByKey().take(10))
        {
            System.out.println(s);
        }
       */
       	return prova5;

    }



    private static JavaPairRDD<String, Tuple2<Double, Double>> CalculateAvgStDevTip2(JavaRDD<Tuple4<LocalDateTime, Long, Double, Double>> rdd2) {

        JavaPairRDD<String, Double> rddAvgTip = rdd2.mapToPair(
                word -> {
                    LocalDateTime odt = word._1();
                    //Integer key = odt.getHour();
                    String key = getDateHour(odt);

                    //Tuple2<Double,Integer> value = new Tuple2<>(word._3(),1);

                    return new Tuple2<>(key, word._4());
                });

        JavaPairRDD<String, Tuple2<Double, Double>> output = rddAvgTip
                .aggregateByKey(
                        new StatCounter(),
                        StatCounter::merge,
                        StatCounter::merge)
                .mapToPair(x -> new Tuple2<>(x._1(), new Tuple2<>( x._2().mean(),x._2().stdev())))
                .sortByKey();

        return output;
    }


    public static String getDateHour(LocalDateTime odt) {
        //String tpep_pickup_datetime = word._1().toString();
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
