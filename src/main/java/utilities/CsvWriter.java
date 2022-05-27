package utilities;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class CsvWriter {

   public static void main(String[] args) throws IOException {
    //public static void CSVMain() {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("Query 1")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        String pathParquet1 = "hdfs://hdfs-namenode:9000/data/yellow_tripdata_2021-12.parquet";
        //String pathParquet2 = "hdfs://hdfs-namenode:9000/data/yellow_tripdata_2022-01.parquet";
        //String pathParquet3 = "hdfs://hdfs-namenode:9000/data/yellow_tripdata_2022-02.parquet";

        List<String> pathList = new ArrayList<>();
        pathList.add(pathParquet1);
        //pathList.add(pathParquet2);
        //pathList.add(pathParquet3);

        /*
        for (String path : pathList) {
            convertParquetToCSV(path, spark);
        }

        mergeCsv();

         */

    }


        public static void convertParquetToCSV(String path, SparkSession spark) {

        Dataset<Row> parquetFileDF = spark.read().parquet(path);

        //.printSchema();

        //convert to csv
        String outputFile = path.replace("parquet", "csv");
        parquetFileDF.write().option("header","true").csv(outputFile);

        //todo: mettere file su  hdfs
    }

    public static void writeQuery1Results2(List<Tuple2<String,Double>> resultList) {
        try {
            FileWriter csvWriter = new FileWriter("output/outputQuery1.csv");
            csvWriter.append("Month");
            csvWriter.append(",");
            csvWriter.append("avg tip/(total amount - toll amount)");
            csvWriter.append("\n");

            for (Tuple2<String,Double> tuple : resultList) {
                csvWriter.append(tuple._1);
                csvWriter.append(",");
                csvWriter.append(Double.toString(tuple._2));
                csvWriter.append("\n");
            }
            csvWriter.flush();
            csvWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public static void writeQuery1Results(JavaPairRDD<String, Double> resultsRDD) {
        try {
            FileWriter csvWriter = new FileWriter("output/outputQuery1.csv");
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

    //merge file csv presi da HDFS
    public static void mergeCsv() throws IOException {
    //public static void main(String[] args) throws IOException {
        // todo: mettere path di HDFS
        String pathCsv1 ="/home/martina/Documents/data/csv/yellow_tripdata_2021-12.csv";
        String pathCsv2 ="/home/martina/Documents/data/csv/yellow_tripdata_2022-01.csv";
        String pathCsv3 ="/home/martina/Documents/data/csv/yellow_tripdata_2022-02.csv";
        String pathOutput = "/home/martina/Documents/data/outputMerge.csv";

        List<String> pathList = new ArrayList<>();
        pathList.add(pathCsv1);
        pathList.add(pathCsv2);
        pathList.add(pathCsv3);

        List<String> mergedLines = new ArrayList<>();
        for (String p : pathList){
            List<String> lines = Files.readAllLines(Path.of(p), Charset.forName("UTF-8"));
            if (!lines.isEmpty()) {

                mergedLines.addAll(lines.subList(1, lines.size()));
            }
        }

        Files.write(Path.of(pathOutput), mergedLines, Charset.forName("UTF-8"));
        }

        /*
        private static List<String> getMergedLines(List<String> paths) throws IOException {
            List<String> mergedLines = new ArrayList<> ();
            for (Path p : paths){
                List<String> lines = Files.readAllLines(p, Charset.forName("UTF-8"));
                if (!lines.isEmpty()) {
                    if (mergedLines.isEmpty()) {
                        mergedLines.add(lines.get(0)); //add header only once
                    }
                    mergedLines.addAll(lines.subList(1, lines.size()));
                }
            }
            return mergedLines;

    }

         */

    public static void writeQ2Results(JavaPairRDD<String, Tuple2<Tuple2<Integer, Tuple2<Double, Double>>, Double>> rdd) {
        try {
            FileWriter csvWriter = new FileWriter("output/outputQuery2.csv");
            csvWriter.append("Day and Hour");
            csvWriter.append(",");
            csvWriter.append("Distribution of the number of trips");
            csvWriter.append(",");
            csvWriter.append("Average tip");
            csvWriter.append(",");
            csvWriter.append("Standard Deviation tip");
            csvWriter.append(",");
            csvWriter.append("Most popular payment");
            csvWriter.append("\n");
            for (Tuple2<String, Tuple2<Tuple2<Integer, Tuple2<Double, Double>>, Double>> i : rdd.collect()){
                csvWriter.append(i._1());                   //data
                csvWriter.append(",");
                Tuple2<Tuple2<Integer, Tuple2<Double, Double>>, Double> tuple = i._2();
                Integer distribution = tuple._1()._1();     //distribution
                csvWriter.append(String.valueOf(distribution));
                csvWriter.append(",");
                Double avgTip = tuple._1._2()._1();         //Average tip
                csvWriter.append(String.valueOf(avgTip));
                csvWriter.append(",");
                Double sdvTip = tuple._1._2()._2();         //SDV tip
                csvWriter.append(String.valueOf(sdvTip));
                csvWriter.append(",");
                Double mostPaym = tuple._2();               //Most popular payment
                csvWriter.append(String.valueOf(mostPaym));
                csvWriter.append("\n");
            }
            csvWriter.flush();
            csvWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
