package query1;

import org.apache.spark.api.java.JavaRDD;
import scala.Tuple3;

public class Query1Preprocessing {

    public static JavaRDD<Tuple3<Double, Double, Double>> preprocData(JavaRDD<String> dataset) {

        // remove header
        String header = dataset.first();

        return dataset.filter(x -> !(x.contains(header) & !(x.contains("NaN")))).map(
                row -> {
                    String[] myFields = row.split(",");
                    //System.out.println("tip == " + myFields[14] + "toll == " + myFields[15] + "tot == " +myFields[17]);
                    Double tip = Double.valueOf(myFields[13]);
                    Double toll = Double.valueOf(myFields[14]);
                    Double tot = Double.valueOf(myFields[16]);
                    //if ( !(Double.isNaN(tip)) & !(Double.isNaN(toll)) & !(Double.isNaN(tot))) {
                    return new Tuple3<>(tip, toll, tot);
                   // }

                }
        ).filter( x -> !(Double.isNaN(x._1())) & !(Double.isNaN(x._2())) & !(Double.isNaN(x._3())) );
    }
}
