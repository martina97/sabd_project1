package queries;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.util.StatCounter;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import utils.QueriesPreprocessing;
import utils.Tuple2Comparator;


import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.Map;

import static utils.convertProva.getMaxOccurence2;

public class Query2 {


    public static void query2Main(JavaRDD<String> rdd) {
        //public static void main(String[] args) {
        JavaRDD<Tuple4<OffsetDateTime, Long, Double, Double>> rdd2 = QueriesPreprocessing.Query2Preprocessing2(rdd).cache();
        //JavaRDD<Tuple3<OffsetDateTime, Double, Double>> rdd2 = QueriesPreprocessing.preprocData(rdd);
        //System.out.println("dopo preproc == " + rdd2.count());


        // --------  Calcolo Distribution of the number of trips distrNumbTripPerH  --------
        //JavaPairRDD<Integer, Integer> distrNumbTripPerH = CalculateDistribution(rdd2).sortByKey();

        //JavaPairRDD<String, Iterable<Tuple2<Long, Double>>> distrNumbTripPerH = CalculateDistribution4(rdd2);
        //todo: ordino la lista dei valori quando scrivo il csv !!!!!
        /*
        System.out.println( "------- distrNumbTripPerH ------- ");
        for (Tuple2<Tuple2<String, Long>, Integer> s : distrNumbTripPerH.collect()) {
            System.out.println(s);
        }

         */







        //--------   Calcolo average tip and its standard deviation --------
/*
       JavaPairRDD<String, Tuple2<Double, Double>> avgAndStDevTip2 = CalculateAvgStDevTip2(rdd2);
        System.out.println( "------- avgAndStDevTip ------- ");
        for (Tuple2<String, Tuple2<Double, Double>> s : avgAndStDevTip2.collect()) {
            System.out.println(s);
        }

 */



        // -------- Calcolo the most popular payment method --------

      // JavaPairRDD<String, Tuple2<Double, Integer>> mostPopularPayment = CalculateTopPayment3(rdd2);
      CalculateTopPaymentComparator(rdd2);
        System.out.println( "------- mostPopularPayment ------- ");
        /*

        for (Tuple2<String, Tuple2<Double, Integer>> s : mostPopularPayment.sortByKey().collect()) {
            System.out.println(s);
        }

         */



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

    private static JavaPairRDD<String, Iterable<Tuple2<Long, Integer>>> CalculateDistribution3(JavaRDD<Tuple4<OffsetDateTime, Long, Double, Double>> rdd) {
        System.out.println(" --------------- CalculateDistribution ----------------");



        //questo rdd contiene coppie (2021-11-30 hh, 1)
        JavaPairRDD<Tuple2<String, Long>, Integer> prova = rdd.mapToPair(
                word -> {

                    //String tpep_pickup_datetime = word._1().toString();
                    OffsetDateTime odt = word._1();
                    String dateHour = getDateHour(odt);
                    Tuple2<String, Long> key = new Tuple2<>(dateHour, word._2());
                    //Integer key = odt.getHour();
                    //System.out.println("key == " + key);

                    //System.out.println("dayOfYear == " + dayOfYear + "hour == " + hour);
                    //String tpep_pickup_datetimeHour = tpep_pickup_datetime.substring(0,13);

                    return new Tuple2<>(key, 1);
                });



        /*
         System.out.println((" ----- prova -------"));
        for (Tuple2<Tuple2<String, Long>, Integer> s: prova.collect()) {
            System.out.println(s);
        }

        ((2021-12-01 01,138),1)
        ((2021-12-01 01,238),1)
        ((2021-12-01 01,239),1)
        ((2021-12-01 01,148),1)
        ((2021-12-01 01,231),1)
        ((2021-12-01 01,132),1)
        ((2021-12-01 01,114),1)
        ((2021-12-01 01,143),1)
        ((2021-12-01 01,138),1)
        ((2021-12-01 01,48),1)
        ((2021-12-01 01,234),1)
        ((2021-12-01 01,239),1)
        ((2021-12-01 01,132),1)
        ((2021-12-01 01,138),1)
        ((2022-01-01 01,166),1)
        ((2022-01-01 01,236),1)
        ((2022-01-01 01,141),1)
        ((2022-01-01 01,114),1)
        ((2022-01-01 01,234),1)
        ((2022-01-01 01,246),1)
         */


        // ((2021-12-01 01,239),2)
        JavaPairRDD<Tuple2<String, Long>, Integer> prova2 = prova.reduceByKey((x, y) -> x + y); // We reduce the elements by key (tpep_pickup hour) and count

        /*
        System.out.println(" ----- prova2 ----- ");
        for (Tuple2<Tuple2<String, Long>, Integer> s: prova2.collect()) {
            System.out.println(s);
        }

        ((2022-01-01 01,246),1)
        ((2022-01-01 01,141),1)
        ((2021-12-01 01,234),1)
        ((2022-01-01 01,166),1)
        ((2021-12-01 01,132),2)
        ((2021-12-01 01,239),2)
        ((2022-01-01 01,114),1)
        ((2022-01-01 01,236),1)
        ((2021-12-01 01,238),1)
        ((2022-01-01 01,234),1)
        ((2021-12-01 01,48),1)
        ((2021-12-01 01,138),3)
        ((2021-12-01 01,114),1)
        ((2021-12-01 01,231),1)
        ((2021-12-01 01,143),1)
        ((2021-12-01 01,148),1)
         */




        JavaPairRDD<String, Tuple2<Long, Integer>> prova3 = prova2.mapToPair(
                row -> new Tuple2<>(row._1()._1(), new Tuple2<>(row._1()._2(), row._2()))
        );
        /*
        (2022-01-01 01,(246,1))
        (2022-01-01 01,(141,1))
        (2021-12-01 01,(234,1))
        (2022-01-01 01,(166,1))
        (2021-12-01 01,(132,2))
        (2021-12-01 01,(239,2))
        (2022-01-01 01,(114,1))
        (2022-01-01 01,(236,1))
        (2021-12-01 01,(238,1))
        (2022-01-01 01,(234,1))
        (2021-12-01 01,(48,1))
        (2021-12-01 01,(138,3))
        (2021-12-01 01,(114,1))
        (2021-12-01 01,(231,1))
        (2021-12-01 01,(143,1))
        (2021-12-01 01,(148,1))

        System.out.println(" ----- prova3 ----- ");
        for (Tuple2<String, Tuple2<Long, Integer>> s: prova3.collect()) {
            System.out.println(s);
        }

         */


        JavaPairRDD<String, Iterable<Tuple2<Long, Integer>>> prova4 = prova3.groupByKey();
        System.out.println(" ----- prova4 ----- ");
        /*
         (2021-12-01 01,[(234,1), (132,2), (239,2), (238,1), (48,1), (138,3), (114,1), (231,1), (143,1), (148,1)])
          (2022-01-01 01,[(246,1), (141,1), (166,1), (114,1), (236,1), (234,1)])

         */


        /* TODO: SERVE QUANDO SCRIVO CSV!!!!!!!
        for (Tuple2<String, Iterable<Tuple2<Long, Integer>>> s: prova4.collect()) {
            System.out.println(s);
            TreeMap<Long, Integer> map = new TreeMap<>();
            Iterator<Tuple2<Long, Integer>> iterator = s._2().iterator();
            while (iterator.hasNext()) {
                Tuple2<Long, Integer> row = iterator.next();
                map.put(row._1(),row._2()); //in questo modo quando scrivo csv basta fare row.get(chiave == zona)
            }

            for (Long elem : map.keySet()) {
                System.out.println("(" + elem + "," + map.get(elem) + ")");
            }
        }

         */




        return prova4;
    }


    private static JavaPairRDD<String, Iterable<Tuple2<Long, Double>>> CalculateDistribution4(JavaRDD<Tuple4<OffsetDateTime, Long, Double, Double>> rdd) {
        System.out.println(" --------------- CalculateDistribution ----------------");

        JavaPairRDD<String, Tuple2<Long, Integer>> prova = rdd.mapToPair(
                word -> {

                    //String tpep_pickup_datetime = word._1().toString();
                    OffsetDateTime odt = word._1();
                    String dateHour = getDateHour(odt);
                    Tuple2<Long, Integer> value = new Tuple2<>(word._2(), 1);
                    //Integer key = odt.getHour();
                    //System.out.println("key == " + key);

                    //System.out.println("dayOfYear == " + dayOfYear + "hour == " + hour);
                    //String tpep_pickup_datetimeHour = tpep_pickup_datetime.substring(0,13);

                    return new Tuple2<>(dateHour, value);
                });
        System.out.println(" ---- prova ------ ");
        for (Tuple2<String, Tuple2<Long, Integer>> s : prova.collect()) {
            System.out.println(s);
        }

        Map<String, Long> map = prova.countByKey();
        System.out.println(" map === " + map);


        JavaPairRDD<Tuple2<String, Long>, Integer>  prova2 = prova.mapToPair(row -> new Tuple2(new Tuple2<>(row._1, row._2._1), row._2._2));
        for (Tuple2<Tuple2<String, Long>, Integer> s : prova2.collect()) {
            System.out.println(s);
        }

        JavaPairRDD<Tuple2<String, Long>, Integer> prova3 = prova2.reduceByKey((x, y) -> x + y); // We reduce the elements by key (tpep_pickup hour) and count
        System.out.println(" ----- prova3 ----- ");
        for (Tuple2<Tuple2<String, Long>, Integer> s: prova3.collect()) {
            System.out.println(s);
        }

        JavaPairRDD<String, Tuple2<Long, Double>> prova4 = prova3.mapToPair(
                row -> {
                    //long perc =Math.round(row._2 * 100/ (float)map.get(row._1._1));
                    double perc = new BigDecimal(row._2 * 100 / (float) map.get(row._1._1)).setScale(2, RoundingMode.HALF_UP).doubleValue(); //arrotondo a 2 cifre decimali

                    System.out.println(" occ == " + map.get(row._1._1));
                    System.out.println(" val == " + row._2 + ", zone == " + row._1()._2() + ", perc === " + perc);
                    //Long occ = map.get(row._1._1);
                   // System.out.println(" occ === " + occ);
                    return new Tuple2<>(row._1()._1(), new Tuple2<>(row._1()._2(), perc));
                }
        );
        System.out.println(" ----- prova4 ----- ");
        for (Tuple2<String, Tuple2<Long, Double>> s: prova4.collect()) {
            System.out.println(s);
        }
        System.out.println(" ----- prova5 ----- ");

        JavaPairRDD<String, Iterable<Tuple2<Long, Double>>> prova5 = prova4.groupByKey();
        for (Tuple2<String, Iterable<Tuple2<Long, Double>>> s: prova5.collect()) {
            System.out.println(s);
        }
        return prova5;
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
    private static JavaPairRDD<String, Tuple2<Double, Integer>> CalculateTopPayment3(JavaRDD<Tuple4<OffsetDateTime, Long, Double, Double>> rdd) {
        System.out.println(" --------------- CalculateTopPayment ----------------");

        // ((ora,pagamento),1) --> ((1,1.0),1)
        JavaPairRDD<Tuple2<String, Double>, Integer> rddAvgTip = rdd.mapToPair(
                row -> {
                    OffsetDateTime odt = row._1();
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
        System.out.println( " ----- reduced ------ ");

        JavaPairRDD<Tuple2<String, Double>, Integer> reduced = rddAvgTip.reduceByKey(
                (a, b) -> a + b
        );
        /*
        for (Tuple2<Tuple2<Integer, Double>, Integer> s : reduced.take(10)) {
            System.out.println(s);
        }

         */


        System.out.println( " ----- boh ------ ");
        // (ora, (pagamento, numero occorrenze) --> (1,(1.0,45))
        JavaPairRDD<String , Tuple2<Double, Integer>> boh = reduced.mapToPair(
                row -> new Tuple2<>(row._1()._1(), new Tuple2<>(row._1()._2(), row._2())));

        /*
        for (Tuple2<Integer, Tuple2<Double, Integer>> s : boh.take(10)) {
            System.out.println(s);
        }

         */


        System.out.println( " ----- boh2 ------ ");
        // (ora, lista(pagamento, numero occorrenze)) --> (1,[(4.0,550), (1.0,61138), (2.0,16766), (3.0,476), (0.0,2951)])
        JavaPairRDD<String, Iterable<Tuple2<Double, Integer>>> boh2 = boh.groupByKey();
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
        JavaPairRDD<String, Tuple2<Double, Integer>> finale = boh2.mapToPair(
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

    private static void CalculateTopPaymentComparator(JavaRDD<Tuple4<OffsetDateTime, Long, Double, Double>> rdd) {
        System.out.println(" --------------- CalculateTopPayment ----------------");

        // ((ora,pagamento),1) --> ((1,1.0),1)
        JavaPairRDD<Tuple2<String, Double>, Integer> rddAvgTip = rdd.mapToPair(
                row -> {
                    OffsetDateTime odt = row._1();
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
        System.out.println( " ----- reduced ------ ");

        JavaPairRDD<Tuple2<String, Double>, Integer> reduced = rddAvgTip.reduceByKey(
                (a, b) -> a + b
        );
        /*
        for (Tuple2<Tuple2<Integer, Double>, Integer> s : reduced.take(10)) {
            System.out.println(s);
        }

         */


        System.out.println( " ----- boh ------ ");
        // (ora, numero occorrenze), pagamento --> ((1,45), 1.0)
        JavaPairRDD<Tuple2<String, Integer>, Double> boh = reduced.mapToPair(row ->
                new Tuple2<>(new Tuple2(row._1._1, row._2), row._1._2));

        for (Tuple2<Tuple2<String, Integer>, Double> s : boh.collect()) {
            System.out.println(s);
        }

        JavaPairRDD<Tuple2<String, Integer>, Double> prova2 = boh.sortByKey(new Tuple2Comparator());

        System.out.println(" ---- prova2 -----");
        for (Tuple2<Tuple2<String, Integer>, Double> s : prova2.collect())
        {
            System.out.println(s);
        }
        JavaPairRDD<String, Iterable<Tuple2<Integer, Double>>> prova5 = prova2.mapToPair(x -> new Tuple2<>(x._1._1, new Tuple2<>(x._1._2, x._2))).groupByKey();

        System.out.println(" ---- prova5 -----");
        for (Tuple2<String, Iterable<Tuple2<Integer, Double>>> s : prova5.collect())
        {
            System.out.println(s);
        }

    }


    // VECCHIO PRIMA DELLA TRACCIA NUOVA
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

    private static JavaPairRDD<String, Tuple2<Double, Double>> CalculateAvgStDevTip2(JavaRDD<Tuple4<OffsetDateTime, Long, Double, Double>> rdd2) {
        System.out.println(" --------------- CalculateAvgStDevTip2 ----------------");

        JavaPairRDD<String, Double> rddAvgTip = rdd2.mapToPair(
                word -> {
                    OffsetDateTime odt = word._1();
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

    public static String getDateHour(OffsetDateTime odt) {
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
