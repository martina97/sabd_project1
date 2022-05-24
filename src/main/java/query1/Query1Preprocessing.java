package query1;

import org.apache.spark.api.java.JavaRDD;
import scala.Tuple3;
import scala.Tuple4;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

public class Query1Preprocessing {

    public static JavaRDD<Tuple4<OffsetDateTime,Double, Double, Double>> preprocData(JavaRDD<String> dataset) {

        // remove header
        String header = dataset.first();
        //System.out.println("header == " + header);
        return dataset.filter(x -> !(x.contains(header) & !(x.contains("NaN")))).map(
                row -> {
                    String[] myFields = row.split(",");
                    //System.out.println("tip == " + myFields[14] + "toll == " + myFields[15] + "tot == " +myFields[17]);
                    DateTimeFormatter f = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
                    //OffsetDateTime odt = OffsetDateTime.parse( "2012-10-01T09:45:00.000+02:00" );
                    OffsetDateTime date = OffsetDateTime.parse( myFields[1]);
                    Double tip = Double.valueOf(myFields[13]);
                    Double toll = Double.valueOf(myFields[14]);
                    Double tot = Double.valueOf(myFields[16]);
                    return new Tuple4<>(date,tip, toll, tot);


                    //if ( !(Double.isNaN(tip)) & !(Double.isNaN(toll)) & !(Double.isNaN(tot))) {


                   // }

                })
                .filter( x -> !(Double.isNaN(x._2())) & !(Double.isNaN(x._3())) & !(Double.isNaN(x._4())))
                .filter(x-> (x._1().getMonthValue() == 12 & x._1().getYear() == 2021) || x._1().getYear() == 2022 &(x._1().getMonthValue() == 1 || x._1().getMonthValue() == 2));
    }

    public static JavaRDD<Tuple4<OffsetDateTime,Double, Double, Double>> preprocData2(JavaRDD<String> dataset) {

        // remove header
        String header = dataset.first();
        //System.out.println("header == " + header);
        return dataset.filter(x -> !(x.contains(header) & !(x.contains("NaN")))).map(
                        row -> {
                            String[] myFields = row.split(",");
                            //System.out.println("tip == " + myFields[14] + "toll == " + myFields[15] + "tot == " +myFields[17]);
                            DateTimeFormatter f = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
                            //OffsetDateTime odt = OffsetDateTime.parse( "2012-10-01T09:45:00.000+02:00" );
                            OffsetDateTime date = OffsetDateTime.parse( myFields[1]);
                            Double tip = Double.valueOf(myFields[13]);
                            Double toll = Double.valueOf(myFields[14]);
                            Double tot = Double.valueOf(myFields[16]);
                            return new Tuple4<>(date,tip, toll, tot);


                            //if ( !(Double.isNaN(tip)) & !(Double.isNaN(toll)) & !(Double.isNaN(tot))) {


                            // }

                        })
                .filter( x -> !(Double.isNaN(x._2())) & !(Double.isNaN(x._3())) & !(Double.isNaN(x._4())))
                .filter(x-> (x._1().getMonthValue() == 12 & x._1().getYear() == 2021) || x._1().getYear() == 2022 &(x._1().getMonthValue() == 1 || x._1().getMonthValue() == 2));
    }

}
