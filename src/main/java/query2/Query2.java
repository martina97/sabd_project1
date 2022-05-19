package query2;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple3;

public class Query2 {

    private static String pathProva = "/home/martina/Documents/data/csv/provaQuery2.csv";


    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("Query 2")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        JavaRDD<String> rdd = spark.read().csv(pathProva).toJavaRDD().map(
                row -> row.mkString(",")
        );

        // remove header
        String header = rdd.first();

        JavaRDD<Tuple3<Double, Double, Double>> rdd2 =  rdd.filter(x -> !(x.contains(header) & !(x.contains("NaN")))).map(
                row -> {
                    String[] myFields = row.split(",");
                    //System.out.println("tip == " + myFields[14] + "toll == " + myFields[15] + "tot == " +myFields[17]);
                    System.out.println("tpep_pickup_datetime == " + myFields[1]);
                    System.out.println(myFields[1].getClass());
                    Double tip = Double.valueOf(myFields[13]);
                    Double toll = Double.valueOf(myFields[14]);
                    Double tot = Double.valueOf(myFields[16]);
                    //if ( !(Double.isNaN(tip)) & !(Double.isNaN(toll)) & !(Double.isNaN(tot))) {
                    return new Tuple3<>(tip, toll, tot);
                    // }

                }
        );
        rdd2.collect();

        spark.stop();
    }


}
