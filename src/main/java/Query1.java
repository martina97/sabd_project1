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
import scala.Tuple3;

import java.util.Arrays;

public class Query1 {
    private static String pathToFile = "/home/martina/Documents/data/yellow_tripdata_2021-12.parquet";
    private static String pathCsv = "/home/martina/Documents/data/csv/prova_2021_12.csv";


    public static void main(String[] args){

        /*
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Query 1");
        JavaSparkContext sc = new JavaSparkContext(conf);

         */

        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("Query 1")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        //read parquet file
        //todo: scommentare riga sotto
        //Dataset<Row> parquetFileDF = spark.read().parquet(pathToFile);

        //parquetFileDF.printSchema();
        /*
        parquetFileDF.show();
        System.out.println("----\n");
        parquetFileDF.printSchema();

         */


        //convert to csv

        /*
        parquetFileDF.write().mode(SaveMode.Overwrite)
                .csv("data/prova.csv");
         */

        //todo: scommentare riga sotto
        //parquetFileDF.write().option("header","true").csv("data/csv");

        /*
        JavaRDD<String> rawTweets = sc.textFile(pathToFile);
        System.out.println("rawTweets ------ \n");
        for (String i : rawTweets.collect()){
            System.out.println(i);
        }

         */


        JavaRDD<String> rdd = spark.read().csv(pathCsv).toJavaRDD().map(
                row -> {
                    return row.mkString(",");
                }
        );

        System.out.println("rdd ------ \n");
        for (String i : rdd.collect()){
            System.out.println(i);
        }

        // rimuovo header
        String header = rdd.first();
        System.out.println("header === " + header);
        JavaRDD<String> rddNoHeader = rdd.filter(x -> !(x.contains(header)));

        System.out.println("rddNoHeader ------ \n");
        for (String i : rddNoHeader.collect()){
            System.out.println(i);
        }


        JavaRDD<Tuple3<Double, Double, Double>> rdd2 = rdd.filter(x -> !(x.contains(header))).map(
                row -> {
                    String[] myFields = row.split(",");
                    //System.out.println("tip == " + myFields[14] + "toll == " + myFields[15] + "tot == " +myFields[17]);
                    Double tip = Double.valueOf(myFields[13]);
                    Double toll = Double.valueOf(myFields[14]);
                    Double tot = Double.valueOf(myFields[16]);
                    return new Tuple3<>(tip, toll, tot);
                }
        );

        System.out.println("\n\n rdd2 ------ \n");
        for (Tuple3<Double, Double, Double> i : rdd2.collect()){
            System.out.println(i);
        }

        JavaRDD<Double> boh = rdd2.map(
                x -> x._1() / (x._3() - x._2())
        );
        System.out.println("\n\n -------- \n");
        for (Double i : boh.collect()){
            System.out.println(i);
        }

        Double resultQ1 = boh.reduce(
                (x,y) -> x+y
        )/boh.count();
        System.out.println("result === " + resultQ1);


        spark.stop();
    }
}
