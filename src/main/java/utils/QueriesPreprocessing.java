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

/*
    public static String parquetFile1 = "hdfs://hdfs-namenode:9000/data/yellow_tripdata_2021-12.parquet";
    public static String parquetFile2 = "hdfs://hdfs-namenode:9000/data/yellow_tripdata_2022-01.parquet";
    public static String parquetFile3 = "hdfs://hdfs-namenode:9000/data/yellow_tripdata_2022-02.parquet";

 */

    public static String parquetFile1 = "/home/martina/Documents/data/yellow_tripdata_2021-12.parquet";
    public static String parquetFile2 = "/home/martina/Documents/data/yellow_tripdata_2022-01.parquet";
    public static String parquetFile3 = "/home/martina/Documents/data/yellow_tripdata_2022-02.parquet";






    //public static void main(String[] args) {
    public static JavaRDD<String> importParquet(SparkSession spark) {
        Dataset<Row> df = spark.read().parquet(parquetFile1);
        JavaRDD<String> rdd1 = df.toJavaRDD().map(row -> row.mkString(","));

       // System.out.println("count df == " + df.count() + " count rdd1 == " + rdd1.count() + " count df.rdd() == " + df.rdd().count());

        //Dataset<Row> df2 = spark.sqlContext().parquetFile(parquetFile2);
        Dataset<Row> df2 = spark.read().parquet(parquetFile2);

        JavaRDD<String> rdd2 = df2.toJavaRDD().map(row -> row.mkString(","));

        //System.out.println("count df2 == " + df2.count() + " count rdd2 == " + rdd2.count() + " count df2.rdd() == " + df2.rdd().count());

        //Dataset<Row> df3 = spark.sqlContext().parquetFile(parquetFile3);
        Dataset<Row> df3 = spark.read().parquet(parquetFile3);

        JavaRDD<String> rdd3 = df3.toJavaRDD().map(row -> row.mkString(","));

        // System.out.println("count df3 == " + df3.count() + " count rdd3 == " + rdd3.count() + " count df3.rdd() == " + df3.rdd().count());


        /*
        System.out.println("rdd1 ---" + rdd1.count());
        System.out.println("rdd2 ---" + rdd2.count());
        System.out.println("rdd3 ---" + rdd3.count());
         */

        JavaRDD<String> rddFinal = rdd1.union(rdd2).union(rdd3);
        /*
        for (String s: rddFinal.take(10)) {
            System.out.println(s);
        }

         */
        System.out.println("rddFinal ---" + rddFinal.count());

        return rddFinal;

    }


    public static JavaRDD<String> importParquet2(SparkSession spark) {
        Dataset<Row> df = spark.read().parquet(parquetFile1);
        JavaRDD<String> rdd1 = df.toJavaRDD().map(row -> row.mkString(","));
        return rdd1;

    }

    /*  VECCHIO PRIMA DI TRACCIA UFFICIALE
    public static JavaRDD<Tuple4<LocalDateTime, Double, Double, Double>> Query1Preprocessing(JavaRDD<String> dataset) {
        // remove header
        //String header = dataset.first();
        //System.out.println("header == " + header);
        //todo: non rimuovere header con file parquet
        return dataset.filter(x -> !(x.contains("NaN"))).map(
                        row -> {
                            String[] myFields = row.split(",");
                            //System.out.println("tip == " + myFields[14] + "toll == " + myFields[15] + "tot == " +myFields[17]);

                            //System.out.println(myFields[1]);

                            Date temp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS")
                                    .parse(myFields[1]);
                            //System.out.println(temp);
                            LocalDateTime localDate = temp.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
                            int month = localDate.getMonthValue();
                            int hour = localDate.getHour();
                            //System.out.println("month == " + month + ", hour == " + hour);

                            Double tip = Double.valueOf(myFields[13]);
                            Double toll = Double.valueOf(myFields[14]);
                            Double tot = Double.valueOf(myFields[16]);
                            return new Tuple4<>(localDate,tip, toll, tot);
                        })
                .filter( x -> !(Double.isNaN(x._2())) & !(Double.isNaN(x._3())) & !(Double.isNaN(x._4())))
                .filter(x-> (x._1().getMonthValue() == 12 & x._1().getYear() == 2021) || x._1().getYear() == 2022 &(x._1().getMonthValue() == 1 || x._1().getMonthValue() == 2));
    }

     */
    public static JavaRDD<Tuple5<LocalDateTime, Double, Double, Double, Double>> Query1Preprocessing(JavaRDD<String> dataset) {
        // remove header
        //String header = dataset.first();
        //System.out.println("header == " + header);
        //todo: non rimuovere header con file parquet
        return dataset.filter(x -> !(x.contains("NaN"))).map(
                        row -> {
                            String[] myFields = row.split(",");
                            //System.out.println("tip == " + myFields[14] + "toll == " + myFields[15] + "tot == " +myFields[17]);

                            //System.out.println(myFields[1]);

                            Date temp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS")
                                    .parse(myFields[1]);
                            //System.out.println(temp);
                            LocalDateTime localDate = temp.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
                            int month = localDate.getMonthValue();
                            int hour = localDate.getHour();
                            //System.out.println("month == " + month + ", hour == " + hour);

                            //System.out.println("month == " + month + ", hour == " + hour);
                            Double payment_type = Double.valueOf(myFields[9]);
                            Double tip = Double.valueOf(myFields[13]);
                            Double toll = Double.valueOf(myFields[14]);
                            Double tot = Double.valueOf(myFields[16]);
                            return new Tuple5<>(localDate, payment_type, tip, toll, tot);
                        })
                .filter(x -> !(Double.isNaN(x._2())) & !(Double.isNaN(x._3())) & !(Double.isNaN(x._4())))
                .filter(x -> (x._1().getMonthValue() == 12 & x._1().getYear() == 2021) || x._1().getYear() == 2022 & (x._1().getMonthValue() == 1 || x._1().getMonthValue() == 2))
                .filter(x -> x._2() == 1);

    }
    public static JavaRDD<Tuple5<OffsetDateTime, Double, Double, Double, Double>> Query1PreprocessingCSV(JavaRDD<String> dataset) {
        // remove header
        //String header = dataset.first();
        //System.out.println("header == " + header);
        //todo: non rimuovere header con file parquet
        String header = dataset.first();
        //System.out.println("header == " + header);
        return dataset.filter(x -> !(x.contains(header) & !(x.contains("NaN")))).map(
                row -> {
                            String[] myFields = row.split(",");
                            //System.out.println("tip == " + myFields[14] + "toll == " + myFields[15] + "tot == " +myFields[17]);

                            //System.out.println(myFields[1]);

                    OffsetDateTime odt = OffsetDateTime.parse( myFields[1]);

                    //System.out.println("odt == " + odt);

                    //LocalDate ld = LocalDate.parse( myFields[1] , f ) ;
                    //System.out.println("ld == " + ld);
                    OffsetDateTime tpep_pickup_datetime = odt;

                            //System.out.println("month == " + month + ", hour == " + hour);

                            //System.out.println("month == " + month + ", hour == " + hour);
                            Double payment_type = Double.valueOf(myFields[9]);
                            Double tip = Double.valueOf(myFields[13]);
                            Double toll = Double.valueOf(myFields[14]);
                            Double tot = Double.valueOf(myFields[16]);
                            return new Tuple5<>(tpep_pickup_datetime, payment_type, tip, toll, tot);
                        })
                .filter(x -> !(Double.isNaN(x._2())) & !(Double.isNaN(x._3())) & !(Double.isNaN(x._4())))
                .filter(x -> (x._1().getMonthValue() == 12 & x._1().getYear() == 2021) || x._1().getYear() == 2022 & (x._1().getMonthValue() == 1 || x._1().getMonthValue() == 2))
                .filter(x -> x._2() == 1);

    }
    public static JavaRDD<Tuple3<OffsetDateTime, Double, Double>> Query2Preprocessing(JavaRDD<String> dataset) {
        // remove header
        // todo: con i file parquet non si copia l'header, quindi non devo toglierlo !!
       String header = dataset.first();
        //System.out.println("header == " + header);
        return dataset.filter(x -> !(x.contains(header) & !(x.contains("NaN")))).map(
       // return dataset.filter(x -> !(x.contains("NaN"))).map(
                        row -> {
                            String[] myFields = row.split(",");
                            //System.out.println("tip == " + myFields[14] + "toll == " + myFields[15] + "tot == " +myFields[17]);

                            //System.out.println(myFields[1]);

                            OffsetDateTime odt = OffsetDateTime.parse( myFields[1]);

                            //System.out.println("odt == " + odt);

                            //LocalDate ld = LocalDate.parse( myFields[1] , f ) ;
                            //System.out.println("ld == " + ld);
                            OffsetDateTime tpep_pickup_datetime = odt;

                            /* todo: scommentare con file parquet
                            Date temp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS")
                                    .parse(myFields[1]);
                            //System.out.println(temp);
                            LocalDateTime tpep_pickup_datetime = temp.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();


                            int month = tpep_pickup_datetime.getMonthValue();
                            int hour = tpep_pickup_datetime.getHour();

                             */
                            //System.out.println("month == " + month + ", hour == " + hour);

                            Double tip_amount = Double.valueOf(myFields[13]);
                            Double payment_type = Double.valueOf(myFields[9]);
                            return new Tuple3<>(tpep_pickup_datetime, payment_type,tip_amount);
                        })
                .filter( x -> !(Double.isNaN(x._2())) & !(Double.isNaN(x._3())))
                .filter(x-> (x._1().getMonthValue() == 12 & x._1().getYear() == 2021) || x._1().getYear() == 2022 &(x._1().getMonthValue() == 1 || x._1().getMonthValue() == 2));
    }

    public static JavaRDD<Tuple3<OffsetDateTime, Double, Double>> preprocData(JavaRDD<String> rdd) {
        // remove header
        String header = rdd.first();

        return rdd.filter(x -> !(x.contains(header) & !(x.contains("NaN")))).map(
                row -> {
                    String[] myFields = row.split(",");
                    //System.out.println("tip == " + myFields[14] + "toll == " + myFields[15] + "tot == " +myFields[17]);
                    //System.out.println("tpep_pickup_datetime == " + myFields[1]);
                    //System.out.println(myFields[1].getClass());
                    //OffsetDateTime odt = OffsetDateTime.parse( "2012-10-01T09:45:00.000+02:00" );
                    OffsetDateTime odt = OffsetDateTime.parse( myFields[1]);

                    //System.out.println("odt == " + odt);

                    //LocalDate ld = LocalDate.parse( myFields[1] , f ) ;
                    //System.out.println("ld == " + ld);
                    OffsetDateTime tpep_pickup_datetime = odt;
                    //System.out.println("tpep_pickup_datetime == " + odt);
                    //System.out.println(tpep_pickup_datetime.getClass());
                    Double tip_amount = Double.valueOf(myFields[13]);
                    Double payment_type = Double.valueOf(myFields[9]);
                    //if ( !(Double.isNaN(tip)) & !(Double.isNaN(toll)) & !(Double.isNaN(tot))) {
                    return new Tuple3<>(tpep_pickup_datetime, payment_type,tip_amount);
                    // }

                }
        );
    }

    public static JavaRDD<Tuple4<OffsetDateTime, Long, Double, Double>> Query2Preprocessing2(JavaRDD<String> dataset) {
        // remove header
        // todo: con i file parquet non si copia l'header, quindi non devo toglierlo !!
        String header = dataset.first();
        //System.out.println("header == " + header);
        return dataset.filter(x -> !(x.contains(header) & !(x.contains("NaN")))).map(
                        // return dataset.filter(x -> !(x.contains("NaN"))).map(
                        row -> {
                            String[] myFields = row.split(",");
                            //System.out.println("tip == " + myFields[14] + "toll == " + myFields[15] + "tot == " +myFields[17]);

                            //System.out.println(myFields[1]);

                            OffsetDateTime odt = OffsetDateTime.parse( myFields[1]);

                            //System.out.println("odt == " + odt);

                            //LocalDate ld = LocalDate.parse( myFields[1] , f ) ;
                            //System.out.println("ld == " + ld);
                            OffsetDateTime tpep_pickup_datetime = odt;

                            /* todo: scommentare con file parquet
                            Date temp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS")
                                    .parse(myFields[1]);
                            //System.out.println(temp);
                            LocalDateTime tpep_pickup_datetime = temp.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();


                            int month = tpep_pickup_datetime.getMonthValue();
                            int hour = tpep_pickup_datetime.getHour();

                             */
                            //System.out.println("month == " + month + ", hour == " + hour);
                            long PULocationID = Long.parseLong(myFields[7]);
                            Double payment_type = Double.valueOf(myFields[9]);
                            Double tip_amount = Double.valueOf(myFields[13]);
                            return new Tuple4<>(tpep_pickup_datetime,PULocationID, payment_type,tip_amount);
                        })
                .filter( x -> !(Double.isNaN(x._3())) & !(Double.isNaN(x._4())))
                .filter(x-> (x._1().getMonthValue() == 12 & x._1().getYear() == 2021) || x._1().getYear() == 2022 &(x._1().getMonthValue() == 1 || x._1().getMonthValue() == 2));

    }
}
