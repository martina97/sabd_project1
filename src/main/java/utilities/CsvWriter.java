package utilities;

import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class CsvWriter {


    public static void writeQuery1Results(List<Tuple2<String,Double>> resultList) {
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

    //public static void mergeCsv(List<String> pathList) throws IOException {
    public static void main(String[] args) throws IOException {
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

    public static void writeQ2Results(JavaPairRDD<String, Integer> rdd) {
        try {
            FileWriter csvWriter = new FileWriter("output/outputQuery2.csv");
            csvWriter.append("Day and Hour");
            csvWriter.append(",");
            csvWriter.append("Distribution of the number of trips");
            csvWriter.append("\n");
            for (Tuple2<String, Integer> i : rdd.collect()){
                csvWriter.append(i._1());
                csvWriter.append(",");
                csvWriter.append(String.valueOf(i._2()));
                csvWriter.append("\n");
            }
            csvWriter.flush();
            csvWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
