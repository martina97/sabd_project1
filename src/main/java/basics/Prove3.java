package basics;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.StatCounter;
import query1.Query1Preprocessing;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import utilities.CsvWriter;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
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
import scala.Tuple4;
import utilities.CsvWriter;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class Prove3 {

        private static String finalPath = "/home/martina/Documents/data/csv/output.csv";
        public static void main(String[] args) throws IOException {

            SparkSession spark = SparkSession.builder()
                    .master("local")
                    .appName("Query 1")
                    .getOrCreate();

            spark.sparkContext().setLogLevel("ERROR");



            JavaRDD<String> rdd = spark.read().csv(finalPath).toJavaRDD().map(
                    row -> row.mkString(",")
            );


            JavaRDD<Tuple4<OffsetDateTime,Double, Double, Double>> rdd2 = Query1Preprocessing.preprocData(rdd);
            //System.out.println("count dopo preproc == "  + rdd2.count());



            computeResults(rdd2);

        /*
        Double resultQ1csv1 = computeResultQ1(pathCsv1, spark);
        Double resultQ1csv2 = computeResultQ1(pathCsv2, spark);
        Double resultQ1csv3 = computeResultQ1(pathCsv3, spark);



        List<Tuple2<String,Double>> resultList = new ArrayList<>();
        resultList.add(new Tuple2<>("2021_12", resultQ1csv1));
        resultList.add(new Tuple2<>("2022_01", resultQ1csv2));
        resultList.add(new Tuple2<>("2022_02", resultQ1csv3));
        CsvWriter.writeQuery1Results(resultList);

         */



            spark.stop();
        }


        private static void computeResults(JavaRDD<Tuple4<OffsetDateTime,Double, Double, Double>> rdd) {
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

            for (Tuple2<String, Double> i : output.collect()){
                System.out.println(i);
            }
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

    }


