package query1;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.shaded.org.eclipse.jetty.websocket.common.frames.DataFrame;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;
import utilities.CsvWriter;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Query1 {
    private static String pathParquet1 = "/home/martina/Documents/data/yellow_tripdata_2021-12.parquet";
    private static String pathParquet2 = "/home/martina/Documents/data/yellow_tripdata_2022-01.parquet";
    private static String pathParquet3 = "/home/martina/Documents/data/yellow_tripdata_2022-02.parquet";

    private static String pathCsv = "/home/martina/Documents/data/csv/prova_2021_12.csv";
    private static String pathCsv1 ="/home/martina/Documents/data/csv/yellow_tripdata_2021-12.csv";
    private static String pathCsv2 ="/home/martina/Documents/data/csv/yellow_tripdata_2022-01.csv";
    private static String pathCsv3 ="/home/martina/Documents/data/csv/yellow_tripdata_2022-02.csv";

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("Query 1")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        //read parquet file
        //todo: scommentare riga sotto
        //Dataset<Row> parquetFileDF = spark.read().parquet(pathParquet1);

        //parquetFileDF.printSchema();
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

        Double resultQ1csv1 = computeResultQ1(pathCsv1, spark);
        Double resultQ1csv2 = computeResultQ1(pathCsv2, spark);
        Double resultQ1csv3 = computeResultQ1(pathCsv3, spark);

        List<Tuple2<String,Double>> resultList = new ArrayList<>();
        resultList.add(new Tuple2<>("2021_12", resultQ1csv1));
        resultList.add(new Tuple2<>("2022_01", resultQ1csv2));
        resultList.add(new Tuple2<>("2022_02", resultQ1csv3));
        CsvWriter.writeQuery1Results(resultList);



        spark.stop();
    }

    private static Double computeResultQ1(String pathCsv1, SparkSession spark) {
        JavaRDD<String> rdd = spark.read().csv(pathCsv1).toJavaRDD().map(
                row -> row.mkString(",")
        );


        /*
        System.out.println("rdd ------ \n");
        for (String i : rdd.collect()){
            System.out.println(i);
        }

         */


        JavaRDD<Tuple3<Double, Double, Double>> rdd2 = Query1Preprocessing.preprocData(rdd);

        /*
        System.out.println("\n\n rdd2 ------ \n");
        for (Tuple3<Double, Double, Double> i : rdd2.collect()){
            System.out.println(i);
        }

         */

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

        JavaRDD<Double> boh = rdd2.map(
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
}
