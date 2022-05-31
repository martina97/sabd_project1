package utils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class convertProva {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Hello World");
        JavaSparkContext sc = new JavaSparkContext(conf);


        // 4 frasi
        JavaRDD<String> input = sc.parallelize(Arrays.asList(
                "1,1",
                "1,2",
                "1,3",
                "1,2",
                "2,1",
                "2,1",
                "2,1",
                "2,4",
                "3,4",
                "3,5"
        ));

        for (String s : input.collect()) {
            System.out.println(s);
        }


        JavaRDD<Tuple2<String, String>> rdd = input.map(
                row -> {
                    String[] myFields = row.split(",");
                    return new Tuple2<>(myFields[0], myFields[1]);
                });







        System.out.println("-----");
        for (Tuple2<String, String> s : rdd.collect()) {
            System.out.println(s);
        }

        JavaPairRDD<String, Tuple2<String, Integer>> rdd2 = rdd.mapToPair(
                row -> {
                    String key = row._1();
                    Tuple2<String, Integer> value = new Tuple2<>(row._2(), 1);
                    return new Tuple2<>(key, value);
                }
        );
        System.out.println("-----");
        for (Tuple2<String, Tuple2<String, Integer>> s : rdd2.collect()) {
            System.out.println(s);
        }


        JavaPairRDD<Tuple2<String,String>, Integer> flatMap = rdd.mapToPair(
                row -> {
                    Tuple2<String, String> key = new Tuple2<>(row._1(), row._2());
                    return new Tuple2<>(key, 1);
                });

        System.out.println("---     flatMap     ---");
        for (Tuple2<Tuple2<String, String>, Integer> s : flatMap.collect()) {
            System.out.println(s);
        }

        JavaPairRDD<Tuple2<String, String>, Integer> reduced = flatMap.reduceByKey(
                (a, b) -> a + b
        );
        System.out.println("---     reduced     ---");
        for (Tuple2<Tuple2<String, String>, Integer> s : reduced.collect()) {
            System.out.println(s);
        }


        JavaPairRDD<String, Tuple2<String, Integer>> boh = reduced.mapToPair(
                row -> {
                    return new Tuple2<>(row._1()._1(), new Tuple2<>(row._1()._2(), row._2()));
                });

        System.out.println("---     boh     ---");
        for (Tuple2<String, Tuple2<String, Integer>> s : boh.collect()) {
            System.out.println(s);
        }

        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> boh2 = boh.groupByKey();


        System.out.println("---     boh2     ---");
        Tuple2<String, Integer> maxTuple = null;
        Integer maxOccurrence = 0;
        for (Tuple2<String, Iterable<Tuple2<String, Integer>>> s : boh2.collect()) {
            System.out.println(s);
            Iterator<Tuple2<String, Integer>> iterator = s._2().iterator();

            while (iterator.hasNext())  {
                Tuple2<String, Integer> currTuple = iterator.next();
                Integer currOccurrence = currTuple._2();
                System.out.println("numOcc == " + currOccurrence);
                if (currOccurrence >= maxOccurrence) {
                    maxOccurrence = currOccurrence;
                    maxTuple = currTuple;
                }

            }


        }
        System.out.println("maxTuple === " + maxTuple);
        JavaPairRDD<String, Tuple2<String, Integer>> finale = boh2.mapToPair(
                elem -> new Tuple2<>(elem._1(), getMaxOccurence(elem._2()))
        );
        System.out.println("---     finale     ---");
        for (Tuple2<String, Tuple2<String, Integer>> s : finale.collect()) {
            System.out.println(s);
        }








    }

    private static Tuple2<String, Integer> getMaxOccurence(Iterable<Tuple2<String, Integer>> iterable) {
        Iterator<Tuple2<String, Integer>> iterator = iterable.iterator();

        Tuple2<String, Integer> maxTuple = null;
        Integer maxOccurrence = 0;
        while (iterator.hasNext())  {
            Tuple2<String, Integer> currTuple = iterator.next();
            Integer currOccurrence = currTuple._2();
            //System.out.println("numOcc == " + currOccurrence);
            if (currOccurrence >= maxOccurrence) {
                maxOccurrence = currOccurrence;
                maxTuple = currTuple;
            }
        }

        return maxTuple;
    }
    public static Tuple2<Double, Integer> getMaxOccurence2(Iterable<Tuple2<Double, Integer>> iterable) {
        Iterator<Tuple2<Double, Integer>> iterator = iterable.iterator();

        Tuple2<Double, Integer> maxTuple = null;
        Integer maxOccurrence = 0;
        while (iterator.hasNext())  {
            Tuple2<Double, Integer> currTuple = iterator.next();
            Integer currOccurrence = currTuple._2();
            //System.out.println("numOcc == " + currOccurrence);
            if (currOccurrence >= maxOccurrence) {
                maxOccurrence = currOccurrence;
                maxTuple = currTuple;
            }
        }

        return maxTuple;
    }


}
