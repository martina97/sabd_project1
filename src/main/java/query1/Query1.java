package query1;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.StatCounter;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import utilities.CsvWriter;

import java.io.IOException;
import java.time.OffsetDateTime;

public class Query1 {
    private static String pathParquet1 = "/home/martina/Documents/data/yellow_tripdata_2021-12.parquet";
    private static String pathParquet2 = "/home/martina/Documents/data/yellow_tripdata_2022-01.parquet";
    private static String pathParquet3 = "/home/martina/Documents/data/yellow_tripdata_2022-02.parquet";

    private static String pathCsv = "/home/martina/Documents/data/csv/prova_2021_12.csv";
    private static String pathCsv1 ="/home/martina/Documents/data/csv/yellow_tripdata_2021-12.csv";
    private static String pathCsv2 ="/home/martina/Documents/data/csv/yellow_tripdata_2022-01.csv";
    private static String pathCsv3 ="/home/martina/Documents/data/csv/yellow_tripdata_2022-02.csv";
    //private static String finalPath = "/home/martina/Documents/data/outputMerge.csv";
    private static String finalPath = "/home/martina/Documents/data/csv/output.csv";
    public static void main(String[] args) throws IOException {

        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("Query 1")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        //read parquet file
        //todo: scommentare riga sotto
        //Dataset<Row> parquetFileDF = spark.read().parquet(pathParquet1);

        //.printSchema();
        /*
        parquetFileDF.show();
        System.out.println("----\n");
        parquetFileDF.printSchema();

         */


        //convert to csv

        //todo: scommentare righe sotto
        /*
        parquetFileDF.write().option("header","true").csv("/home/martina/Documents/data/csv/");

        Dataset<Row> parquetFileDF2 = spark.read().parquet(pathParquet2);
        parquetFileDF2.write().option("header","true").csv("/home/martina/Documents/data/csv/2");
        Dataset<Row> parquetFileDF3 = spark.read().parquet(pathParquet3);
        parquetFileDF3.write().option("header","true").csv("/home/martina/Documents/data/csv/3");

         */


        /*
        JavaRDD<String> rawTweets = sc.textFile(pathToFile);
        System.out.println("rawTweets ------ \n");
        for (String i : rawTweets.collect()){
            System.out.println(i);
        }

         */

        JavaRDD<String> rdd = spark.read().csv(finalPath).toJavaRDD().map(
                row -> row.mkString(",")
        );

        /*
        JavaRDD<String> rdd1 = spark.read().csv(pathCsv1).toJavaRDD().map(
                row -> row.mkString(",")
        );
        JavaRDD<String> rdd2 = spark.read().csv(pathCsv2).toJavaRDD().map(
                row -> row.mkString(",")
        );
        JavaRDD<String> rdd3 = spark.read().csv(pathCsv3).toJavaRDD().map(
                row -> row.mkString(",")
        );

         */


        JavaRDD<Tuple4<OffsetDateTime,Double, Double, Double>> rdd2 = Query1Preprocessing.preprocData(rdd);
        //System.out.println("count dopo preproc == "  + rdd2.count());

        JavaPairRDD<String, Double> resultRDD = computeResults(rdd2);
        CsvWriter.writeQuery1Results(resultRDD);

        //todo: divido in 3 rdd e calcolo separatamente, ma ci mette piu tempo!
        /*
        // RDD con dati di dicembre 2021
        JavaRDD<Tuple3<Double,Double,Double>> rddDec = rdd2
                .filter( line -> line._1().getMonthValue() == 12)
                .map( line -> new Tuple3<>(line._2(), line._3(), line._4())
        );


        // RDD con dati di gennaio 2022
        JavaRDD<Tuple3<Double,Double,Double>> rddJan = rdd2
                .filter( line -> line._1().getMonthValue() == 1)
                .map( line -> new Tuple3<>(line._2(), line._3(), line._4())
                );

        // RDD con dati di febbraio 2022
        JavaRDD<Tuple3<Double,Double,Double>> rddFeb = rdd2
                .filter( line -> line._1().getMonthValue() == 2)
                .map( line -> new Tuple3<>(line._2(), line._3(), line._4())
                );

        Double resultQ1Dec = computeResultQ1(rddDec);
        Double resultQ1Jan = computeResultQ1(rddJan);
        Double resultQ1Feb = computeResultQ1(rddFeb);

        List<Tuple2<String,Double>> resultList = new ArrayList<>();
        resultList.add(new Tuple2<>("2021_12", resultQ1Dec));
        resultList.add(new Tuple2<>("2022_01", resultQ1Jan));
        resultList.add(new Tuple2<>("2022_02", resultQ1Feb));
        CsvWriter.writeQuery1Results(resultList);

         */



        spark.stop();
    }



    private static Double computeResultQ1(JavaRDD<Tuple3<Double,Double,Double>> rdd) {


        //CONTROLLO SE CI SONO NaN
        /*
        long numeroNaN2 = rdd2.filter(x -> Double.isNaN(x._1())).count();
        long numeroNaN3 = rdd2.filter(x -> Double.isNaN(x._2())).count();
        long numeroNaN4 = rdd2.filter(x -> Double.isNaN(x._3())).count();
        System.out.println("numeroNaN2 == " + numeroNaN2);
        System.out.println("numeroNaN3 == " + numeroNaN3);
        System.out.println("numeroNaN4 == " + numeroNaN4);

         */

        //non ci sono nan, vengono generati dopo evidentemente per qualche operazione di divisione
        // infatti 0.0 / 0.0 = NaN (basta vedere quanto vale Double.NaN
        //quindi devo levare i NaN successivamente

        JavaRDD<Double> boh = rdd.map(
                x -> x._1() / (x._3() - x._2())
        );
        System.out.println("\n\n -------- \n");

/*
        BufferedWriter writer = new BufferedWriter(new FileWriter("/home/martina/Documents/data/output.txt"));

        for (Double i : boh.collect()){
            //System.out.println(i);
            writer.write(String.valueOf(i));
        }


        writer.close();

 */





        //System.out.println("count valori con nan == " + boh.count());

        /*
        long numeroNaN = boh.filter(x -> Double.isNaN(x)).count();
        System.out.println("numeroNaN == " + numeroNaN);

         */

        JavaRDD<Double> rddWithoutNaN = boh
                .filter(x -> !(Double.isNaN(x)));

        Double resultQ1 = rddWithoutNaN
                .reduce((x,y) -> x+y)
                /rddWithoutNaN.count();

/*
        Double somma = rddWithoutNaN.reduce( (sum, element) -> {
            //System.out.println("dentro reduce sum == " + sum + " elem == " + element );
            sum = sum + element;
            //System.out.println("dentro reduce sum == " + sum);
            return sum;
        });

 */

        //long count = rddWithoutNaN.count();
        System.out.println("result == " + resultQ1);
        //System.out.println("somma == " + somma + " count == " + count + " res == " + somma/count);
        //System.out.println("result === " + resultQ1);
        return resultQ1;


    }

    // PIU EFFICIENTE, CI METTE MENO
    // todo: vedere differenza di tempo tra questo metodo e l'altro
    private static JavaPairRDD<String, Double> computeResults(JavaRDD<Tuple4<OffsetDateTime,Double, Double, Double>> rdd) {
        // voglio pair RDD con key = 2021-12, value = tip/(total amount - toll amount)

        JavaPairRDD<String, Double> rddAvgTip = rdd.mapToPair(
                word -> {
                    OffsetDateTime odt = word._1();
                    String key = odt.getYear() + "-" + odt.getMonthValue();
                    Double value = word._2() / (word._4()- word._3());
                    //Tuple2<Double,Integer> value = new Tuple2<>(word._3(),1);

                    return new Tuple2<>(key, value);
                });
        JavaPairRDD<String, Double> output = rddAvgTip
                .filter(x-> !(Double.isNaN(x._2()))) //rimuovo NaN generati da tip/(total amount - toll amount) (infatti 0.0/0.0 = NaN)
                .aggregateByKey(
                        new StatCounter(),
                        StatCounter::merge,
                        StatCounter::merge)
                .mapToPair(x -> new Tuple2<>(x._1(),  x._2().mean()))
                .sortByKey();

        return output;
    }

}
