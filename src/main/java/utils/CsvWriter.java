package utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple4;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class CsvWriter {

    public static String pathQuery1Results = "results/resultsQuery1.csv";
    public static String pathQuery2Results = "results/resultsQuery2.csv";
    public static String pathQuery1SQLResults = "results/resultsQuery1SQL.csv";
    public static String pathQuery3Results = "results/resultsQuery3.csv";


   public static void writeQuery2(JavaPairRDD<String, Tuple2<Tuple2<Iterable<Tuple2<Long, Double>>, Tuple2<Double, Double>>, Iterable<Tuple2<Integer, Double>>>> resultQ2 ) {
        try {

            // scrittura su hdfs
            Configuration configuration = new Configuration();
            configuration.set("fs.defaultFS","hdfs://hdfs-namenode:9000");
            FileSystem hdfs;
            hdfs = FileSystem.get(configuration);
            Path outputPathHDFS = new Path("hdfs://hdfs-namenode:9000/"+ pathQuery2Results);
            FSDataOutputStream fsDataOutputStream;
            fsDataOutputStream = hdfs.create(outputPathHDFS,true);
            BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            sb.append("YYYY-MM-DD-HH");
            sb.append(",");
            for(int i = 1; i<266;i++)  {
                sb.append("perc_PU"+i);
                sb.append(",");
            }
            sb.append("avg_tip");
            sb.append(",");
            sb.append("stddev_tip");
            sb.append(",");
            sb.append("pref_payment");
            sb.append("\n");



            FileWriter csvWriter = new FileWriter("/docker/node_volume/resultsQuery2.csv");


            csvWriter.append("YYYY-MM-DD-HH");
            csvWriter.append(",");
            for(int i = 1; i<266;i++)  {
                csvWriter.append("perc_PU"+i);
                csvWriter.append(",");
            }
            csvWriter.append("avg_tip");
            csvWriter.append(",");
            csvWriter.append("stddev_tip");
            csvWriter.append(",");
            csvWriter.append("pref_payment");
            csvWriter.append("\n");



            for (Tuple2<String, Tuple2<Tuple2<Iterable<Tuple2<Long, Double>>, Tuple2<Double, Double>>, Iterable<Tuple2<Integer, Double>>>> tuple : resultQ2.collect()) {

                sb.append(tuple._1);
                sb.append(",");
                csvWriter.append(tuple._1);
                csvWriter.append(",");
                ArrayList<Tuple2<Long, Double>> list = Lists.newArrayList(tuple._2._1._1);
                Map<Long, Double> resultMap = list.stream().collect(Collectors.toMap(Tuple2::_1, Tuple2::_2));
                for(long j = 1; j<266;j++)  {
                    if (resultMap.get(j) == null) {
                        sb.append(0);
                        csvWriter.append(String.valueOf(0));

                    } else {
                        sb.append(resultMap.get(j));
                        csvWriter.append(String.valueOf(resultMap.get(j)));
                    }
                    sb.append(",");
                    csvWriter.append(",");

                }
                sb.append(tuple._2._1._2._1);
                sb.append(",");
                sb.append(tuple._2._1._2._2);
                sb.append(",");
                sb.append(Iterables.get(tuple._2._2, 0)._2);
                sb.append("\n");

                csvWriter.append(String.valueOf(tuple._2._1._2._1));
                csvWriter.append(",");
                csvWriter.append(String.valueOf(tuple._2._1._2._2));
                csvWriter.append(",");
                csvWriter.append(String.valueOf(Iterables.get(tuple._2._2,0)._2));
                csvWriter.append("\n");

            }
            csvWriter.flush();
            csvWriter.close();
            bufferedWriter.write(sb.toString());
            bufferedWriter.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void writeQuery1HDFS_CSV(JavaPairRDD<String, Tuple2<Double, Long>> resultsRDD) {

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
            sb.append("YYYY-MM");
            sb.append(",");

            sb.append("tip_percentage");
            sb.append(",");

            sb.append("trips_number");
            sb.append('\n');


            for (Tuple2<String, Tuple2<Double, Long>> tuple : resultsRDD.collect()) {
                sb.append(tuple._1);
                sb.append(",");
                sb.append(tuple._2()._1);
                sb.append(",");
                sb.append(tuple._2()._2);
                sb.append("\n");

            }

            bufferedWriter.write(sb.toString());
            bufferedWriter.close();
          } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void writeQuery1ResultsCSV(JavaPairRDD<String, Tuple2<Double, Long>> resultsRDD) {
        try {
            // scrittura su csv locale
            FileWriter csvWriter = new FileWriter(pathQuery1Results);

            csvWriter.append("YYYY-MM");
            csvWriter.append(",");
            csvWriter.append("tip_percentage");
            csvWriter.append(",");
            csvWriter.append("trips_number");
            csvWriter.append("\n");

            for (Tuple2<String, Tuple2<Double, Long>> tuple : resultsRDD.collect()) {


                csvWriter.append(tuple._1());
                csvWriter.append(",");
                csvWriter.append(Double.toString(tuple._2()._1));
                csvWriter.append(",");
                csvWriter.append(Double.toString(tuple._2()._2));
                csvWriter.append("\n");
            }
            csvWriter.flush();
            csvWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void writeQuery1SQL(Dataset<Row> result) {
        try {
		 // scrittura su hdfs
            Configuration configuration = new Configuration();
            configuration.set("fs.defaultFS","hdfs://hdfs-namenode:9000");
            FileSystem hdfs = null;
            hdfs = FileSystem.get(configuration);
            Path outputPathHDFS = new Path("hdfs://hdfs-namenode:9000/"+ pathQuery1SQLResults);
            FSDataOutputStream fsDataOutputStream = null;
            fsDataOutputStream = hdfs.create(outputPathHDFS,true);
            BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8));

            StringBuilder sb = new StringBuilder();
            sb.append("YYYY-MM");
            sb.append(",");

            sb.append("tip_percentage");
            sb.append(",");

            sb.append("trips_number");
            sb.append('\n');




            FileWriter csvWriter = new FileWriter("/docker/node_volume/resultsQuery1SQL.csv");
            csvWriter.append("YYYY-MM");
            csvWriter.append(",");

            csvWriter.append("tip_percentage");
            csvWriter.append(",");

            csvWriter.append("trips_number");
            csvWriter.append('\n');


            for (Row row : result.collectAsList()) {
	   	        sb.append(row.getString(0));
                sb.append(",");
                sb.append(row.getDouble(1));
                sb.append(",");
                sb.append(row.getLong(2));
                sb.append("/n");

                csvWriter.append(row.getString(0));
                csvWriter.append(",");
                csvWriter.append(String.valueOf(row.getDouble(1)));
                csvWriter.append(",");
                csvWriter.append(String.valueOf(row.getLong(2)));
                csvWriter.append("/n");
	    
	    }
            csvWriter.flush();
            csvWriter.close();
	    bufferedWriter.write(sb.toString());
            bufferedWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public static void writeQuery3(JavaPairRDD<String, ArrayList<Tuple4<Long, Double, Double, Double>>> result) {
            try {
                // scrittura su hdfs
                Configuration configuration = new Configuration();
                configuration.set("fs.defaultFS","hdfs://hdfs-namenode:9000");
                FileSystem hdfs;
                hdfs = FileSystem.get(configuration);
                Path outputPathHDFS = new Path("hdfs://hdfs-namenode:9000/"+ pathQuery3Results);
                FSDataOutputStream fsDataOutputStream;
                fsDataOutputStream = hdfs.create(outputPathHDFS,true);
                BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8));
                StringBuilder sb = new StringBuilder();


                FileWriter csvWriter = new FileWriter("/docker/node_volume/resultsQuery3.csv");

                csvWriter.append("YYYY-MM-DD");
                csvWriter.append(",");
                sb.append("YYYY-MM-DD");
                sb.append(",");

                HashMap<Long,String> map = ZoneMap.createZoneMap();
                for (int i = 0; i<6; i++) {
                    csvWriter.append("DO" + i);
                    csvWriter.append(",");
                    csvWriter.append("avg_pax_DO" + i);
                    csvWriter.append(",");
                    csvWriter.append("avg_fare_DO" + i);
                    csvWriter.append(",");
                    csvWriter.append("stddev_fare_DO" + i);

                    sb.append("DO" + i);
                    sb.append(",");
                    sb.append("avg_pax_DO" + i);
                    sb.append(",");
                    sb.append("avg_fare_DO" + i);
                    sb.append(",");
                    sb.append("stddev_fare_DO" + i);
                    if (i != 5) {
                        csvWriter.append(',');
                        sb.append(',');
                    } else {
                        csvWriter.append('\n');
                        sb.append('\n');
                    }
                }
                for( Tuple2<String, ArrayList<Tuple4<Long, Double, Double, Double>>> s : result.collect()) {
                    System.out.println(s);
                    csvWriter.append(s._1);
                    csvWriter.append(',');
                    sb.append(s._1);
                    sb.append(',');
                    ArrayList<Tuple4<Long, Double, Double, Double>> list = s._2;
                    for (int j = 0; j < 5; j++) {
                        Long location = list.get(j)._1();
                        csvWriter.append(String.valueOf(map.get(location)));
                        csvWriter.append(",");
                        csvWriter.append(String.valueOf(list.get(j)._4()));
                        csvWriter.append(",");
                        csvWriter.append(String.valueOf(list.get(j)._3()));
                        csvWriter.append(",");
                        csvWriter.append(String.valueOf(list.get(j)._2()));

                        sb.append(String.valueOf(map.get(location)));
                        sb.append(",");
                        sb.append(String.valueOf(list.get(j)._4()));
                        sb.append(",");
                        sb.append(String.valueOf(list.get(j)._3()));
                        sb.append(",");
                        sb.append(String.valueOf(list.get(j)._2()));
                        if (j != 4) {
                            csvWriter.append(",");
                            sb.append(",");
                        } else {
                            csvWriter.append("\n");
                            sb.append("\n");
                        }
                    }
                }

                bufferedWriter.write(sb.toString());
                bufferedWriter.close();
                csvWriter.flush();
                csvWriter.close();

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
}
