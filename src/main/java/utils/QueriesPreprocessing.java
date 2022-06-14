package utils;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple5;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Date;

public class QueriesPreprocessing {


    public static String pattern = "yyyy-MM-dd HH:mm:ss.SSSSSS";
    public static String parquetFile1 = "hdfs://hdfs-namenode:9000/data/yellow_tripdata_2021-12.parquet";
    public static String parquetFile2 = "hdfs://hdfs-namenode:9000/data/yellow_tripdata_2022-01.parquet";
    public static String parquetFile3 = "hdfs://hdfs-namenode:9000/data/yellow_tripdata_2022-02.parquet";


    public static JavaRDD<String> importParquet(SparkSession spark) {
        Dataset<Row> df = spark.read().parquet(parquetFile1);
        Dataset<Row> df2 = spark.read().parquet(parquetFile2);
        Dataset<Row> df3 = spark.read().parquet(parquetFile3);

        Dataset<Row> dfRes = df.union(df2).union(df3);

        JavaRDD<String> rddRes = dfRes.toJavaRDD().map(row -> row.mkString(","));

        return rddRes;

    }


    public static JavaRDD<Tuple5<LocalDateTime, Double, Double, Double, Double>> Query1Preprocessing(JavaRDD<String> dataset) {
      
        return dataset.filter(x -> !(x.contains("NaN"))).map(
                        row -> {
                            String[] myFields = row.split(",");

                            Date temp = new SimpleDateFormat(pattern).parse(myFields[1]);
                            LocalDateTime localDate = temp.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();

                            Double payment_type = Double.valueOf(myFields[9]);
                            Double tip = Double.valueOf(myFields[13]);
                            Double toll = Double.valueOf(myFields[14]);
                            Double tot = Double.valueOf(myFields[16]);
                            return new Tuple5<>(localDate, payment_type, tip, toll, tot);
                        })
                .filter(x -> !(Double.isNaN(x._2())) & !(Double.isNaN(x._3())) & !(Double.isNaN(x._4())))
                .filter(x -> (x._1().getMonthValue() == 12 & x._1().getYear() == 2021) || x._1().getYear() == 2022 & (x._1().getMonthValue() == 1 || x._1().getMonthValue() == 2))
                .filter(x -> x._2() == 1);  // take credit card payments only

    }
   
    public static JavaRDD<Tuple4<LocalDateTime, Long, Double, Double>> Query2Preprocessing(JavaRDD<String> dataset) {

        return dataset.filter(x ->  !(x.contains("NaN"))).map(
                        row -> {
                            String[] myFields = row.split(",");
                            Date temp = new SimpleDateFormat(pattern).parse(myFields[1]);

                            LocalDateTime tpep_pickup_datetime = temp.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();

                            long PULocationID = Long.parseLong(myFields[7]);
                            Double payment_type = Double.valueOf(myFields[9]);
                            Double tip_amount = Double.valueOf(myFields[13]);
                            return new Tuple4<>(tpep_pickup_datetime,PULocationID, payment_type,tip_amount);
                        })
                .filter( x -> !(Double.isNaN(x._3())) & !(Double.isNaN(x._4())))
                .filter(x-> (x._1().getMonthValue() == 12 & x._1().getYear() == 2021) || x._1().getYear() == 2022 &(x._1().getMonthValue() == 1 || x._1().getMonthValue() == 2));

    }




 

  
    public static JavaRDD<Tuple4<LocalDateTime, Double, Long, Double>> Query3Preprocessing(JavaRDD<String> dataset) {

        return dataset.filter(x ->  !(x.contains("NaN") & !(x.contains(",,")))).map(
                        row -> {
                            String[] myFields = row.split(",");

                            Date temp = new SimpleDateFormat(pattern).parse(myFields[1]);
                            LocalDateTime tpep_pickup_datetime = temp.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();

                            double passenger_count;
                            if (myFields[3].equals("null")) {
                                passenger_count = Double.NaN;
                            } else {
                                passenger_count = Double.parseDouble(myFields[3]);
                            }

                            long DOLocationID= Long.parseLong(myFields[8]);
                            Double fare_amount= Double.valueOf(myFields[10]);
                            return new Tuple4<>(tpep_pickup_datetime,passenger_count,DOLocationID, fare_amount);
                        })
                .filter( x -> !(Double.isNaN(x._2())) & !(Double.isNaN(x._4())))
                .filter(x-> (x._1().getMonthValue() == 12 & x._1().getYear() == 2021) || x._1().getYear() == 2022 &(x._1().getMonthValue() == 1 || x._1().getMonthValue() == 2));

    }
}
