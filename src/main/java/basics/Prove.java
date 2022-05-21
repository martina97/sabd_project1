package basics;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Prove {


        public static void main(String[] args) {
            SparkConf conf = new SparkConf().setAppName("Average Calculation").setMaster("local[2]");
            JavaSparkContext sc = new JavaSparkContext(conf);
            //inputList
            List<Tuple2<String,Integer>> inputList = new ArrayList<Tuple2<String,Integer>>();
            inputList.add(new Tuple2<String,Integer>("a1", 30));
            inputList.add(new Tuple2<String,Integer>("b1", 30));
            inputList.add(new Tuple2<String,Integer>("a1", 40));
            inputList.add(new Tuple2<String,Integer>("a1", 20));
            inputList.add(new Tuple2<String,Integer>("b1", 50));
            //parallelizePairs
            JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(inputList);
            System.out.println("---------------------\n\npairRDD");

            for (Tuple2<String, Integer> i : pairRDD.collect()){
                System.out.println(i);
            }
            System.out.println("---------------------\n\nvalueCount");
            //count each values per key
            JavaPairRDD<String, Tuple2<Integer, Integer>> valueCount = pairRDD.mapValues(value -> new Tuple2<Integer, Integer>(value,1));
            for (Tuple2<String, Tuple2<Integer, Integer>> i : valueCount.collect()){
                System.out.println(i);
            }
            System.out.println("---------------------\n\nreducedCount");
            //add values by reduceByKey
            JavaPairRDD<String, Tuple2<Integer, Integer>> reducedCount = valueCount.reduceByKey((tuple1,tuple2) ->  new Tuple2<Integer, Integer>(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2));
            for (Tuple2<String, Tuple2<Integer, Integer>> i : reducedCount.collect()){
                System.out.println(i);
            }
            System.out.println("---------------------\n\naveragePair");
            //calculate average
            JavaPairRDD<String, Integer> averagePair = reducedCount.mapToPair(getAverageByKey);
            for (Tuple2<String, Integer> i : averagePair.collect()){
                System.out.println(i);
            }
            //print averageByKey
            averagePair.foreach(data -> {
                System.out.println("Key="+data._1() + " Average=" + data._2());
            });
            //stop sc
            sc.stop();
            sc.close();
        }

        private static PairFunction<Tuple2<String, Tuple2<Integer, Integer>>,String,Integer> getAverageByKey = (tuple) -> {
            Tuple2<Integer, Integer> val = tuple._2;
            int total = val._1;
            int count = val._2;
            Tuple2<String, Integer> averagePair = new Tuple2<String, Integer>(tuple._1, total / count);
            return averagePair;
        };

}
