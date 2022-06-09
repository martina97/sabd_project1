package queries;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;
import scala.Tuple4;
import utils.QueriesPreprocessing;

import java.time.OffsetDateTime;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

public class Query3 {

    public static void query3Main(JavaRDD<String> rdd) {
        JavaRDD<Tuple4<OffsetDateTime, Double, Long, Double>> rddPreproc = QueriesPreprocessing.Query3Preprocessing(rdd);
        for (Tuple4<OffsetDateTime, Double, Long, Double> s : rddPreproc.collect()) {
            System.out.println(s);
        }


        mostPopularZones2(rddPreproc);
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

    private static void mostPopularZones2(JavaRDD<Tuple4<OffsetDateTime, Double, Long, Double>> rddPreproc) {
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

        JavaPairRDD<Tuple2<String, Integer>, Long> prova2 = prova.sortByKey();
        System.out.println(" \n---- prova2 -----");
        for (Tuple2<Tuple2<String, Integer>, Long> s : prova2.collect()) {
            System.out.println(s);
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
}
