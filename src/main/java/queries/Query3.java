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

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeMap;

public class Query3 {

    public static void query3Main(JavaRDD<String> rdd) {


        // (tpep_pickup_datetime, passenger_count, DOLocationID, fare_amount)
        JavaRDD<Tuple4<LocalDateTime, Double, Long, Double>> rddPreproc = QueriesPreprocessing.Query3Preprocessing(rdd);

        mostPopularZones3(rddPreproc);
    }

    private static void mostPopularZones(JavaRDD<Tuple4<OffsetDateTime, Double, Long, Double>> rddPreproc) {
        JavaPairRDD<String, Tuple2<Long, Integer>> prova = rddPreproc.mapToPair(
                        word -> {
                            String date = word._1().getYear() + "-" + word._1().getMonthValue() + "-" + word._1().getDayOfMonth();
                            Tuple2<String, Long> key = new Tuple2<>(date, word._3());
                            return new Tuple2<>(key, 1);
                        }
                ).reduceByKey((x, y) -> x + y)
                .mapToPair(row -> new Tuple2<>(row._1._1, new Tuple2<>(row._1._2, row._2)));

        System.out.println(" \n---- prova -----");
        for (Tuple2<String, Tuple2<Long, Integer>> s : prova.collect()) {
            System.out.println(s);
        }

        JavaPairRDD<String, Iterable<Tuple2<Long, Integer>>> prova2 = prova.groupByKey();
        System.out.println(" \n---- prova2 -----");

        for (Tuple2<String, Iterable<Tuple2<Long, Integer>>> s : prova2.collect()) {
            System.out.println(s);
        }
        JavaPairRDD<String, Iterable<Tuple2<Long, Integer>>> prova3 = prova2.sortByKey(false);
        System.out.println(" \n---- prova3 -----");
        for (Tuple2<String, Iterable<Tuple2<Long, Integer>>> s : prova3.collect()) {

            TreeMap map = new TreeMap();
            //System.out.println(s);
            System.out.println(s._2());
            Iterator<Tuple2<Long, Integer>> iterator = s._2.iterator();
            while (iterator.hasNext()) {
                Tuple2<Long, Integer> elem = iterator.next();
                map.put(elem._2, elem._1);
            }
            System.out.println(" map == " + map);

        }

        /*
        List<Iterable<Tuple2<Long, Integer>>> boh = prova3.lookup("2021-12-1");
        System.out.println("boh == " + boh);

         */

    }

    private static void mostPopularZones2(JavaRDD<Tuple4<LocalDateTime, Double, Long, Double>> rddPreproc) {
        // (giorno, DOLocationID)
        JavaPairRDD<Tuple2<String, Integer>, Long> prova = rddPreproc.mapToPair(
                        word -> {
                            String date = word._1().getYear() + "-" + word._1().getMonthValue() + "-" + word._1().getDayOfMonth();
                            Tuple2<String, Long> key = new Tuple2<>(date, word._3());
                            return new Tuple2<>(key, 1);
                        }
                ).reduceByKey((x, y) -> x + y)
                .mapToPair(row -> new Tuple2<>(new Tuple2<>(row._1._1, row._2), row._1._2));

        System.out.println(" \n---- prova -----");
        for (Tuple2<Tuple2<String, Integer>, Long> s : prova.collect()) {
            System.out.println(s);
        }

        JavaPairRDD<Tuple2<String, Integer>, Long> prova2 = prova.sortByKey(new Tuple2Comparator());
        System.out.println(" \n---- prova2 -----");
        for (Tuple2<Tuple2<String, Integer>, Long> s : prova2.collect()) {
            System.out.println(s);
        }

        JavaPairRDD<String, Iterable<Tuple2<Integer, Long>>> prova5 = prova2
                .mapToPair(x -> new Tuple2<>(x._1._1, new Tuple2<>(x._1._2, x._2)))
                .groupByKey();

        System.out.println(" ---- prova5 -----");
        for (Tuple2<String, Iterable<Tuple2<Integer, Long>>> s : prova5.take(10))
        {
            System.out.println(s);
        }

        JavaPairRDD<String, ArrayList<Tuple2<Integer, Long>>> prova6 = prova5
                .mapToPair(x -> {
                    //System.out.println("iterable == " + x._2);
                    //System.out.println("Iterables.limit == " + Iterables.limit(x._2, 2));
                    Iterable<Tuple2<Integer, Long>> provaa = Iterables.limit(x._2, 5);
                    //System.out.println("provaaa = " + provaa);
                    return new Tuple2<>(x._1, Lists.newArrayList(provaa));
                });



        System.out.println(" ---- prova6 -----");
        for (Tuple2<String, ArrayList<Tuple2<Integer, Long>>> s : prova6.take(10))
        {
            System.out.println(s);
           // System.out.println("iterable == " + s._2);
           // System.out.println("Iterables.limit == " +Iterables.limit(s._2(), 5));
        }
/*
        JavaPairRDD<Tuple2<String, Integer>, Iterable<Long>> prova2 = prova.groupByKey();
        System.out.println(" \n---- prova2 -----");

        for (Tuple2<Tuple2<String, Integer>, Iterable<Long>> s : prova2.collect()) {
            System.out.println(s);
        }
        JavaPairRDD<String, Iterable<Tuple2<Long, Integer>>> prova3 = prova2.sortByKey(false);
        System.out.println(" \n---- prova3 -----");
        for (Tuple2<String, Iterable<Tuple2<Long, Integer>>> s : prova3.collect()) {

            TreeMap map = new TreeMap();
            //System.out.println(s);
            System.out.println(s._2());
            Iterator<Tuple2<Long, Integer>> iterator = s._2.iterator();
            while (iterator.hasNext()) {
                Tuple2<Long, Integer> elem = iterator.next();
                map.put(elem._2, elem._1);
            }
            System.out.println(" map == " + map);

        }

 */

        /*
        List<Iterable<Tuple2<Long, Integer>>> boh = prova3.lookup("2021-12-1");
        System.out.println("boh == " + boh);

         */

    }



    private static void mostPopularZones3(JavaRDD<Tuple4<LocalDateTime, Double, Long, Double>> rddPreproc) {
        // (giorno, DOLocationID)
        // (tpep_pickup_datetime, passenger_count, DOLocationID, fare_amount)

        JavaPairRDD<Tuple2<String, Long>, StatCounter> prova = rddPreproc.mapToPair(
                        word -> {
				
				int day = word._1().getDayOfMonth();
				String dayStr = String.valueOf(day);
				if (day < 10) {
					dayStr = "0"+day;
				}

				int month =  word._1().getMonthValue();
			       String monthStr = String.valueOf(month);
		      		
			       if (month < 10) {
         			   monthStr = "0"+month;
        			}	
                            String date = word._1().getYear() + "-" + monthStr + "-" + dayStr;
                            Tuple2<String, Long> key = new Tuple2<>(date, word._3());
                            Double value = (word._4());
                            return new Tuple2<>(key, value);
                        }
                ).aggregateByKey(
                        new StatCounter(),
                         StatCounter::merge,
                        StatCounter::merge)
        ;
                /*.reduceByKey((x, y) -> x._1() + y._1())
                .mapToPair(row -> new Tuple2<>(new Tuple2<>(row._1._1, row._2), row._1._2));

                 */

		/*
        System.out.println(" \n---- prova -----");
        for (Tuple2<Tuple2<String, Long>, StatCounter> s : prova.collect()) {
            System.out.println(s);
        }*/
/*
        
        // (giorno, (occorrenze, media fare amount, stdev fare amount)
        JavaPairRDD<Tuple2<String, Integer>, Tuple3<Long, Double, Double>> prova2 = prova
                .mapToPair(x -> {
                    // chiave: (giorno, occorrenze)
                    Tuple2<String, Integer> key = new Tuple2<>(x._1._1, (int) x._2.count());
                    // valore: (zona, media fare amount, stdev fare amount)
                    Tuple3<Long, Double, Double> value = new Tuple3<>(x._1._2, x._2.mean(), x._2.stdev());
                    return new Tuple2<>(key, value);
                });


        System.out.println(" \n---- prova2 -----");
        for (Tuple2<Tuple2<String, Integer>, Tuple3<Long, Double, Double>> s : prova2.collect()) {
            System.out.println(s);
        }
        JavaPairRDD<Tuple2<String, Integer>, Tuple3<Long, Double, Double>> prova3 = prova2
                .sortByKey(new Tuple2Comparator());
        System.out.println(" \n---- prova3 -----");
        for (Tuple2<Tuple2<String, Integer>, Tuple3<Long, Double, Double>> s : prova3.collect()) {
            System.out.println(s);
        }
        
 */

        System.out.println(" ----------------------- NUM MEDIO PASSEGGERI ---------------------- ");
        JavaPairRDD<Tuple2<String, Long>, Double> rddPassenger = rddPreproc.mapToPair(
                word -> {
			  int day = word._1().getDayOfMonth();
                                String dayStr = String.valueOf(day);
                                if (day < 10) {
                                        dayStr = "0"+day;
                                }

                                int month =  word._1().getMonthValue();
                               String monthStr = String.valueOf(month);

                               if (month < 10) {
                                   monthStr = "0"+month;
                                }
                            String date = word._1().getYear() + "-" + monthStr + "-" + dayStr;

                    //String date = word._1().getYear() + "-" + word._1().getMonthValue() + "-" + word._1().getDayOfMonth();
                    Tuple2<String, Long> key = new Tuple2<>(date, word._3());   //chiave: (giorno, DOLocationID)
                    Double value = (word._2()); // valore: passenger_count
                    return new Tuple2<>(key, value);
                });
        /*System.out.println(" ---- rddPassenger ------ ");
        for (Tuple2<Tuple2<String, Long>, Double> s : rddPassenger.collect()) {
            System.out.println(s);
        }*/


        JavaPairRDD<Tuple2<String, Long>, Double> avgPax = rddPassenger
                .aggregateByKey(
                        new StatCounter(),
                        StatCounter::merge,
                        StatCounter::merge)
                .mapToPair(x -> new Tuple2<>(x._1, x._2.mean()));
        /*System.out.println(" ---- avgPax ------ ");
        for (Tuple2<Tuple2<String, Long>, Double> s : avgPax.collect()) {
            System.out.println(s);
        }
*/
        // (giorno, (occorrenze, media fare amount, stdev fare amount)
        JavaPairRDD<Tuple2<String, Long>, Tuple3<Long, Double, Double>> provaa = prova
                .mapToPair(x -> {
                    // chiave: (giorno, occorrenze)
                    Tuple2<String, Long> key = new Tuple2<>(x._1._1, x._1._2);
                    // valore: (zona, media fare amount, stdev fare amount)x._2.count()
                    Tuple3<Long, Double, Double> value = new Tuple3<>(x._2.count(), x._2.mean(), x._2.stdev());
                    return new Tuple2<>(key, value);
                });
      /*  System.out.println(" ---- provaa ------ ");
        for (Tuple2<Tuple2<String, Long>, Tuple3<Long, Double, Double>> s : provaa.collect()) {
            System.out.println(s);
        }
*/
        // ((2021-12-1,236),((2,21.25,3.25),1.0)) : ((giorno,zona),((occorrenze,media fare_amount,std fare_amount),num medio passeggeri))
        JavaPairRDD<Tuple2<String, Long>, Tuple2<Tuple3<Long, Double, Double>, Double>> rddJoin = provaa.join(avgPax);
     /*   System.out.println(" ---- rddJoin ------ ");
        for (Tuple2<Tuple2<String, Long>, Tuple2<Tuple3<Long, Double, Double>, Double>> s : rddJoin.collect()) {
            System.out.println(s);
        }
*/
        // ora come chiave metto (giorno, occorrenze) in modo da poter ordinare dall'occorrenza maggiore
        JavaPairRDD<Tuple2<String, Integer>, Tuple4> rddJoinSorted = rddJoin.mapToPair(x ->
                {
                    Integer m = Math.toIntExact(x._2._1._1());
                    return new Tuple2<>(new Tuple2<>(x._1._1, m), new Tuple4(x._1._2, x._2._1._2(), x._2._1._3(), x._2._2));

                }
        ).sortByKey(new Tuple2Comparator());

  /*      System.out.println(" ---- rddJoinSorted ------ ");
        for (Tuple2<Tuple2<String, Integer>, Tuple4> s : rddJoinSorted.collect()) {
            System.out.println(s);
        }*/

        JavaPairRDD<String, ArrayList<Tuple4>> finale = rddJoinSorted
                .mapToPair(x -> new Tuple2<>(x._1._1, x._2))
                .groupByKey()
                .mapToPair(x -> {
                    //System.out.println("iterable == " + x._2);
                    //System.out.println("Iterables.limit == " + Iterables.limit(x._2, 2));
                    Iterable<Tuple4> top5Iterable = Iterables.limit(x._2, 5);
                    //System.out.println("provaaa = " + provaa);
                    return new Tuple2<>(x._1, Lists.newArrayList(top5Iterable));
                }).sortByKey();
                
        /*
        JavaPairRDD<String, ArrayList<Tuple4>> finale = rddJoinSorted.groupByKey()
                .mapToPair(x -> {
                    //System.out.println("iterable == " + x._2);
                    //System.out.println("Iterables.limit == " + Iterables.limit(x._2, 2));
                    Iterable<Tuple4> top5Iterable = Iterables.limit(x._2, 5);
                    //System.out.println("provaaa = " + provaa);
                    return new Tuple2<>(x._1._1, Lists.newArrayList(top5Iterable));
                });
                
         */

        System.out.println(" ---- finale ------ ");
        for (Tuple2<String, ArrayList<Tuple4>> s : finale.take(10)) {
            System.out.println(s);
        }
    }
}
