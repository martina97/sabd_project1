package queries;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.util.StatCounter;
import scala.Tuple2;
import scala.Tuple3;
import utils.CsvWriter;
import utils.QueriesPreprocessing;


import java.time.LocalDateTime;
import java.time.OffsetDateTime;

import static utils.convertProva.getMaxOccurence2;

public class Query2 {


    public static void query2Main(JavaRDD<String> rdd) {
        //public static void main(String[] args) {
        JavaRDD<Tuple3<LocalDateTime, Double, Double>> rdd2 = QueriesPreprocessing.Query2Preprocessing(rdd).cache();
        //JavaRDD<Tuple3<OffsetDateTime, Double, Double>> rdd2 = QueriesPreprocessing.preprocData(rdd);
        //System.out.println("dopo preproc == " + rdd2.count());


        // --------  Calcolo Distribution of the number of trips distrNumbTripPerH  --------
        /*
        JavaPairRDD<Integer, Integer> distrNumbTripPerH = CalculateDistribution(rdd2).sortByKey();
        System.out.println( "------- distrNumbTripPerH ------- ");
        for (Tuple2<Integer, Integer> s : distrNumbTripPerH.collect()) {
            System.out.println(s);
        }

         */



        //--------   Calcolo average tip and its standard deviation --------
        /*
       JavaPairRDD<Integer, Tuple2<Double, Double>> avgAndStDevTip2 = CalculateAvgStDevTip2(rdd2);
        System.out.println( "------- avgAndStDevTip ------- ");



        for (Tuple2<Integer, Tuple2<Double, Double>> s : avgAndStDevTip2.collect()) {
            System.out.println(s);
        }

         */



        // -------- Calcolo the most popular payment method --------
       JavaPairRDD<Integer, Tuple2<Double, Integer>> mostPopularPayment = CalculateTopPayment2(rdd2);
        System.out.println( "------- mostPopularPayment ------- ");

        for (Tuple2<Integer, Tuple2<Double, Integer>> s : mostPopularPayment.sortByKey().collect()) {
            System.out.println(s);
        }

        /*
        JavaPairRDD<Integer, Tuple2<Tuple2<Integer, Tuple2<Double, Double>>, Tuple2<Double, Integer>>> resultQ2 = distrNumbTripPerH
                .join(avgAndStDevTip2)
                .join(mostPopularPayment)
                .sortByKey();

        System.out.println(" \n\n------ RESULT Q2 ------- ");
        for (Tuple2<Integer, Tuple2<Tuple2<Integer, Tuple2<Double, Double>>, Tuple2<Double, Integer>>> s : resultQ2.collect()){
            System.out.println(s);
        }

        CsvWriter.writeQuery2(resultQ2);

         */
    }

    private static JavaPairRDD<Integer, Double> CalculateTopPayment (JavaRDD<Tuple3<LocalDateTime, Double, Double>> rdd) {
        System.out.println(" --------------- CalculateTopPayment ----------------");
        JavaPairRDD<Integer,Double> rddAvgTip = rdd.mapToPair(
                word -> {
                    LocalDateTime odt = word._1();
                    Integer key = odt.getHour();

                    return new Tuple2<>(key, word._2());
                });

        JavaPairRDD<Integer, Double> output = rddAvgTip
                .aggregateByKey(
                        new StatCounter(),
                        StatCounter::merge,
                        StatCounter::merge)
                .mapToPair(x -> new Tuple2<>(x._1(),x._2().max()))
                .sortByKey();

        return output;
    }
    private static JavaPairRDD<Integer, Tuple2<Double, Integer>> CalculateTopPayment2(JavaRDD<Tuple3<LocalDateTime, Double, Double>> rdd) {
        System.out.println(" --------------- CalculateTopPayment ----------------");

        // ((ora,pagamento),1) --> ((1,1.0),1)
        JavaPairRDD<Tuple2<Integer,Double>, Integer>  rddAvgTip = rdd.mapToPair(
                row -> {
                    Tuple2<Integer, Double> key = new Tuple2<>(row._1().getHour(), row._2());
                    return new Tuple2<>(key, 1);
                });
        /*
        for (Tuple2<Tuple2<Integer, Double>, Integer> s : rddAvgTip.take(10)) {
            System.out.println(s);
        }

         */

        // (ora, pagamento), numero occorrenze) --> ((1,1.0),45)
        System.out.println( " ----- reduced ------ ");

        JavaPairRDD<Tuple2<Integer, Double>, Integer> reduced = rddAvgTip.reduceByKey(
                (a, b) -> a + b
        );
        /*
        for (Tuple2<Tuple2<Integer, Double>, Integer> s : reduced.take(10)) {
            System.out.println(s);
        }

         */


        System.out.println( " ----- boh ------ ");
        // (ora, (pagamento, numero occorrenze) --> (1,(1.0,45))
        JavaPairRDD<Integer, Tuple2<Double, Integer>> boh = reduced.mapToPair(
                row -> new Tuple2<>(row._1()._1(), new Tuple2<>(row._1()._2(), row._2())));

        /*
        for (Tuple2<Integer, Tuple2<Double, Integer>> s : boh.take(10)) {
            System.out.println(s);
        }

         */


        System.out.println( " ----- boh2 ------ ");
        // (ora, lista(pagamento, numero occorrenze)) --> (1,[(4.0,550), (1.0,61138), (2.0,16766), (3.0,476), (0.0,2951)])
        JavaPairRDD<Integer, Iterable<Tuple2<Double, Integer>>> boh2 = boh.groupByKey();
        /*
        for (Tuple2<Integer, Iterable<Tuple2<Double, Integer>>> s : boh2.collect()) {
            System.out.println(s);
        }

         */


        //calcolo most popular payment
        /*
        Tuple2<Double, Integer> maxTuple = null;
        Integer maxOccurrence = 0;

         */
        JavaPairRDD<Integer, Tuple2<Double, Integer>> finale = boh2.mapToPair(
                elem -> new Tuple2<>(elem._1(), getMaxOccurence2(elem._2()))
        );
        System.out.println("---     finale     ---");
        /*
        for (Tuple2<Integer, Tuple2<Double, Integer>> s : finale.collect()) {
            System.out.println(s);
        }
         */
        return finale;

    }

    private static JavaPairRDD<Integer, Integer> CalculateDistribution(JavaRDD<Tuple3<LocalDateTime, Double, Double>> rdd) {
        System.out.println(" --------------- CalculateDistribution ----------------");


        //questo rdd contiene coppie (2021-11-30 hh, 1)
        return rdd.mapToPair(
                        word -> {
                            //String tpep_pickup_datetime = word._1().toString();
                            LocalDateTime odt = word._1();
                            //String key = getDateHour(odt);
                            Integer key = odt.getHour();
                            //System.out.println("key == " + key);

                            //System.out.println("dayOfYear == " + dayOfYear + "hour == " + hour);
                            //String tpep_pickup_datetimeHour = tpep_pickup_datetime.substring(0,13);

                            return new Tuple2<>(key, 1);
                        })
                .reduceByKey((x, y) -> x+y); // We reduce the elements by key (tpep_pickup hour) and count
        /*
        for (Tuple2<String, Integer> i : pairs2.collect()){
            System.out.println(i);
        }

         */

    }

    private static JavaPairRDD<Integer, Tuple2<Double, Double>> CalculateAvgStDevTip2(JavaRDD<Tuple3<LocalDateTime, Double, Double>> rdd2) {
        System.out.println(" --------------- CalculateAvgStDevTip2 ----------------");

        JavaPairRDD<Integer, Double> rddAvgTip = rdd2.mapToPair(
                word -> {
                    LocalDateTime odt = word._1();
                    Integer key = odt.getHour();
                    //Tuple2<Double,Integer> value = new Tuple2<>(word._3(),1);

                    return new Tuple2<>(key, word._3());
                });

        //JavaRDD<Tuple3<String, Double, Double>> output = rddAvgTip
        JavaPairRDD<Integer, Tuple2<Double,Double>> output = rddAvgTip
                .aggregateByKey(
                        new StatCounter(),
                        StatCounter::merge,
                        StatCounter::merge)
                .mapToPair(x -> new Tuple2<>(x._1(), new Tuple2<>( x._2().mean(),x._2().stdev())))
                .sortByKey();

        return output;
    }

    private static JavaPairRDD<Integer, Integer> CalculateDistribution2(JavaRDD<Tuple3<OffsetDateTime, Double, Double>> rdd) {
        System.out.println(" --------------- CalculateDistribution ----------------");


        //questo rdd contiene coppie (2021-11-30 hh, 1)
        return rdd.mapToPair(
                        word -> {
                            //String tpep_pickup_datetime = word._1().toString();
                            OffsetDateTime odt = word._1();
                            //String key = getDateHour(odt);
                            Integer key = odt.getHour();
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

    private static String getDateHour(LocalDateTime odt) {
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
        String key = odt.getYear() + "-" + month + "-" + day + " " + hour;
        return key;
    }

}
