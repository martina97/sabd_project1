package utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class CsvWriter {

    //public static String parquetFile1 = "./docker/data/yellow_tripdata_2021-12.parquet";
    public static String parquetFile1 = "/home/martina/Documents/data/yellow_tripdata_2021-12.parquet";

   // public static String parquetFile2 = "./docker/data/yellow_tripdata_2022-01.parquet";
    public static String parquetFile2 = "/home/martina/Documents/data/yellow_tripdata_2022-01.parquet";
    //public static String parquetFile3= "./docker/data/yellow_tripdata_2022-02.parquet";
    public static String parquetFile3= "/home/martina/Documents/data/yellow_tripdata_2022-02.parquet";
    private static String finalPath = "/home/martina/Documents/data/csv/output.csv";
    private static String finalPath2 = "./docker/data/output.parquet";



    public static String pathQuery2Results = "results/resultsQuery2.csv";
    public static String pathQuery1Results = "results/resultsQuery1.csv";

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("Query 1")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");


        //URL website = new URL("https://nyc-tlc.s3.amazonaws.com/trip+data/yellow_tripdata_2021-12.parquet");
       // File file = new File(parquetFile1);
       // FileUtils.copyURLToFile(website, file);
        /*

        website = new URL("https://nyc-tlc.s3.amazonaws.com/trip+data/yellow_tripdata_2022-01.parquet");
        file = new File(parquetFile2);
        FileUtils.copyURLToFile(website, file);

        website = new URL("https://nyc-tlc.s3.amazonaws.com/trip+data/yellow_tripdata_2022-02.parquet");
        file = new File(parquetFile3);
        FileUtils.copyURLToFile(website, file);

         */


        JavaRDD<String> rdd =
                //spark.read().csv(finalPath)
                spark.read().csv(finalPath)
                        .toJavaRDD().map(
                                row -> row.mkString(",")
                        );

        System.out.println("count 1 == " + rdd.count());

        //Dataset<Row> df = spark.sqlContext().parquetFile(parquetFile1);
        Dataset<Row> df = spark.read().parquet(parquetFile1);
        JavaRDD<String> rdd1 = df.toJavaRDD().map(row -> row.mkString(","));
        //Dataset<Row> df2 = spark.sqlContext().parquetFile(parquetFile2);
        Dataset<Row> df2 = spark.read().parquet(parquetFile2);
        JavaRDD<String> rdd2 = df2.toJavaRDD().map(row -> row.mkString(","));
        //Dataset<Row> df3 = spark.sqlContext().parquetFile(parquetFile3);
        Dataset<Row> df3 = spark.read().parquet(parquetFile3);
        JavaRDD<String> rdd3 = df3.toJavaRDD().map(row -> row.mkString(","));
        System.out.println("rdd1 ---" + rdd1.count());
        System.out.println("rdd2 ---" + rdd2.count());
        System.out.println("rdd3 ---" + rdd3.count());
        /*
        for (String s : rdd2.take(5)) {
            System.out.println(s);
        }

         */
        /*
        Dataset<Row> dfMerged = df.union(df2).union(df3).distinct();


        JavaRDD<Row> rdd2 = dfMerged.toJavaRDD();

        System.out.println("count 2 == " + rdd2.count());

         */


        /*
        for (Row s : rdd.take(4)) {
            System.out.println(s);
        }

        JavaRDD<String> provaRdd = rdd.map(row -> row.mkString(","));
        System.out.println("----");
        for(String s : provaRdd.take(4) ){
            System.out.println(s);
        }

         */
       // df.printSchema();
       //df.write().format("csv").save("./docker/data/yellow_tripdata_2021-12.csv");



        /*
        Dataset<Row> parquetFileDF = spark.read().parquet(parquetFile1);
        //convert to csv
        String outputFile = "./docker/data/yellow_tripdata_2021-12.csv";
        parquetFileDF.write().option("header","true").csv(outputFile);


        parquetFileDF = spark.read().parquet(parquetFile2);
        //convert to csv
        outputFile = "./docker/data/yellow_tripdata_2022-01.csv";
        parquetFileDF.write().option("header","true").csv(outputFile);

        parquetFileDF = spark.read().parquet(parquetFile3);
        //convert to csv
        outputFile = "./docker/data/yellow_tripdata_2022-02.csv";
        parquetFileDF.write().option("header","true").csv(outputFile);

         */



        //String pathParquet1 = "hdfs://hdfs-namenode:9000/data/yellow_tripdata_2021-12.parquet";
        //String pathParquet2 = "hdfs://hdfs-namenode:9000/data/yellow_tripdata_2022-01.parquet";
        //String pathParquet3 = "hdfs://hdfs-namenode:9000/data/yellow_tripdata_2022-02.parquet";

        /*
        List<String> pathList = new ArrayList<>();
        pathList.add(pathParquet1);

         */
        //pathList.add(pathParquet2);
        //pathList.add(pathParquet3);

        /*
        for (String path : pathList) {
            convertParquetToCSV(path, spark);
        }

        mergeCsv();

         */

    }


    public static void provaCSV() throws IOException {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("Query 1")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");
        String pathParquet1 = "hdfs://hdfs-namenode:9000/data/yellow_tripdata_2021-12.parquet";
        //String output = "hdfs://hdfs-namenode:9000/data/output.parquet";
        Dataset<Row> parquetFileDF = spark.read().parquet(pathParquet1);

        JavaRDD<Row> prova = parquetFileDF.toJavaRDD();
        for (Row r : prova.take(4)) {
            System.out.println(r);
        }
        System.out.println("--fine---");
        spark.stop();
        //Dataset<Row> parquetFileDF2 = spark.read().parquet(output);



    }
    public static void convertParquetToCSV(String path, SparkSession spark) {

        Dataset<Row> parquetFileDF = spark.read().parquet(path);


        //.printSchema();

        //convert to csv
        String outputFile = path.replace("parquet", "csv");
        parquetFileDF.write().option("header","true").csv(outputFile);

        //todo: mettere file su  hdfs
    }

    public static void writeQuery2(JavaPairRDD<Integer, Tuple2<Tuple2<Integer, Tuple2<Double, Double>>, Tuple2<Double, Integer>>> resultQ2 ) {
        try {

            // scrittura su hdfs
            Configuration configuration = new Configuration();
            configuration.set("fs.defaultFS","hdfs://hdfs-namenode:9000");
            FileSystem hdfs = null;
            hdfs = FileSystem.get(configuration);
            Path outputPathHDFS = new Path("hdfs://hdfs-namenode:9000/"+ pathQuery2Results);
            FSDataOutputStream fsDataOutputStream = null;
            fsDataOutputStream = hdfs.create(outputPathHDFS,true);
            BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            sb.append("Hour");
            sb.append(",");
            sb.append("Distribution of the number of trips");
            sb.append(",");
            sb.append("Average tip");
            sb.append(",");
            sb.append("Standard deviation tip");
            sb.append(",");
            sb.append("Most popular payment method");
            sb.append(",");
            sb.append("Most popular payment method occurences");
            sb.append("\n");


            // scrittura su csv locale
            FileWriter csvWriter = new FileWriter(pathQuery2Results);

            csvWriter.append("Hour");
            csvWriter.append(",");
            csvWriter.append("Distribution of the number of trips");
            csvWriter.append(",");
            csvWriter.append("Average tip");
            csvWriter.append(",");
            csvWriter.append("Standard deviation tip");
            csvWriter.append(",");
            csvWriter.append("Most popular payment method");
            csvWriter.append(",");
            csvWriter.append("Most popular payment method occurences");
            csvWriter.append("\n");

            for (Tuple2<Integer, Tuple2<Tuple2<Integer, Tuple2<Double, Double>>, Tuple2<Double, Integer>>> tuple : resultQ2.collect()) {

                csvWriter.append(String.valueOf(tuple._1()));
                csvWriter.append(",");
                csvWriter.append(String.valueOf(tuple._2()._1()._1()));
                csvWriter.append(",");
                csvWriter.append(String.valueOf(tuple._2()._1._2()._1()));
                csvWriter.append(",");
                csvWriter.append(String.valueOf(tuple._2()._1._2()._2()));
                csvWriter.append(",");
                csvWriter.append(String.valueOf(tuple._2()._2()._1()));
                csvWriter.append(",");
                csvWriter.append(String.valueOf(tuple._2()._2()._2()));
                csvWriter.append("\n");

                // hdfs
                sb.append(tuple._1());
                sb.append(",");
                sb.append(tuple._2()._1()._1());
                sb.append(",");
                sb.append(tuple._2()._1._2()._1());
                sb.append(",");
                sb.append(tuple._2()._1._2()._2());
                sb.append(",");
                sb.append(tuple._2()._2()._1());
                sb.append(",");
                sb.append(tuple._2()._2()._2());
                sb.append("\n");
            }
            bufferedWriter.write(sb.toString());
            bufferedWriter.close();
            csvWriter.flush();
            csvWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void writeQuery1(JavaPairRDD<String, Double> resultsRDD) {

        try {
            // scrittura su hdfs
            Configuration configuration = new Configuration();
            configuration.set("fs.defaultFS","hdfs://hdfs-namenode:9000");
            FileSystem hdfs = null;
            hdfs = FileSystem.get(configuration);
            Path outputPathHDFS = new Path("hdfs://hdfs-namenode:9000/"+ pathQuery1Results);
            FSDataOutputStream fsDataOutputStream = null;
            fsDataOutputStream = hdfs.create(outputPathHDFS,true);
            BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8));

            StringBuilder sb = new StringBuilder();
            sb.append("Month");
            sb.append(",");

            sb.append("avg tip/(total amount - toll amount)");
            sb.append('\n');


            // scrittura su csv locale
            FileWriter csvWriter = new FileWriter(pathQuery1Results);

            csvWriter.append("Month");
            csvWriter.append(",");
            csvWriter.append("avg tip/(total amount - toll amount)");
            csvWriter.append("\n");

            for (Tuple2<String, Double> tuple : resultsRDD.collect()) {
                sb.append(tuple._1());
                sb.append(",");
                sb.append(tuple._2());
                sb.append("\n");

                csvWriter.append(tuple._1());
                csvWriter.append(",");
                csvWriter.append(Double.toString(tuple._2()));
                csvWriter.append("\n");
            }

            bufferedWriter.write(sb.toString());
            bufferedWriter.close();
            csvWriter.flush();
            csvWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void writeQuery1Results(JavaPairRDD<String, Double> resultsRDD) {
        try {
            FileWriter csvWriter = new FileWriter(pathQuery1Results);
            csvWriter.append("Month");
            csvWriter.append(",");
            csvWriter.append("avg tip/(total amount - toll amount)");
            csvWriter.append("\n");

            for (Tuple2<String, Double> tuple : resultsRDD.collect()) {
                csvWriter.append(tuple._1());
                csvWriter.append(",");
                csvWriter.append(Double.toString(tuple._2()));
                csvWriter.append("\n");

            }
            csvWriter.flush();
            csvWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
