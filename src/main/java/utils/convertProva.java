package utils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.time.LocalDateTime;
import java.util.*;



public class convertProva {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Hello World");
        JavaSparkContext sc = new JavaSparkContext(conf);

        /*
         * As data source, we can use a file stored on the local file system
         * or on the HDFS, or we can parallelize
         */
//        JavaRDD<String> input = sc.textFile("hdfs://HOST:PORT/input");
//        JavaRDD<String> input = sc.textFile("input");
        JavaRDD<String> input = sc.parallelize(Arrays.asList(
                "a,40",
                "a,20",
                "a,20",
                "a,20",
                "a,30",
                "a,30",
                "b,30",
                "b,10",
                "b,10",
                "a,40",
                "b,40",
                "b,20",
                "c,10",
                "c,10",
                "c,20"
        ));

        for (String s : input.collect())
        {
            System.out.println(s);
        }

        JavaPairRDD<Tuple2<String, Integer>, Integer> prova = input.mapToPair(
                row -> {
                    String[] myFields = row.split(",");
                    return new Tuple2<>(new Tuple2<>(myFields[0], Integer.valueOf(myFields[1])), 1);
                }
        );
        System.out.println(" ---- prova -----");
        for (Tuple2 s : prova.collect())
        {
            System.out.println(s);
        }

        JavaPairRDD<Tuple2<String, Integer>, Integer> prova2 = prova.reduceByKey(
                (a, b) -> a + b
        );
        System.out.println(" ---- prova2 -----");
        for (Tuple2<Tuple2<String, Integer>, Integer> s : prova2.collect())
        {
            System.out.println(s);
        }

        JavaPairRDD<Tuple2<String,Integer>, Integer> prova3 = prova2.mapToPair(row ->
                new Tuple2<>(new Tuple2(row._1._1, row._2), row._1._2));
        System.out.println(" ---- prova3 -----");
        for (Tuple2<Tuple2<String, Integer>, Integer> s : prova3.collect())
        {
            System.out.println(s);
        }
        JavaPairRDD<Tuple2<String, Integer>, Integer> prova4 = prova3.sortByKey(new Tuple2Comparator());

        //JavaPairRDD<Tuple2<String, Integer>, Integer> prova4 = prova3.sortByKey(new Tuple2Comparator(), true, 1);
        System.out.println(" ---- prova4 -----");
        for (Tuple2<Tuple2<String, Integer>, Integer> s : prova4.collect())
        {
            System.out.println(s);
        }

        JavaPairRDD<String, Iterable<Tuple2<Integer, Integer>>> prova5 = prova4.mapToPair(x -> new Tuple2<>(x._1._1, new Tuple2<>(x._1._2, x._2))).groupByKey();

        System.out.println(" ---- prova5 -----");
        for (Tuple2<String, Iterable<Tuple2<Integer, Integer>>> s : prova5.collect())
        {
            System.out.println(s);
        }



        /*
        JavaPairRDD<String, Iterable<Integer>> prova2 = prova.groupByKey();
        System.out.println(" ---- prova2 -----");


        for (Tuple2<String, Iterable<Integer>> s : prova2.collect())
        {
            TreeMap map = new TreeMap();
            //System.out.println(s);
            System.out.println(s._2());
            Iterator<Integer> iterator = s._2.iterator();
            while (iterator.hasNext()) {
                map.
            }

        }

         */



        /*
        long occ = 14;
        int num = 1;

        System.out.println(Math.ceil((num*100)/occ));
        System.out.println(Math.round((num*100)/occ));
        System.out.println(Math.ceil((num*100)/(float)occ));
        System.out.println(Math.round((num*100)/(float)occ));
        DecimalFormat df = new DecimalFormat("0.00");
        BigDecimal bd = new BigDecimal(num*100/(float)occ).setScale(2, RoundingMode.HALF_UP);

        System.out.println(df.format(num*100/(float)occ));
        System.out.println(bd.doubleValue());

         */

    }

        public static void start() {
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
