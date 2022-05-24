package query1;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.codehaus.janino.CodeContext;
import scala.Tuple4;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Query1SQL {
    //private static String finalPath = "prova_2021_12.csv";
    private static String finalPath = "/home/martina/Documents/data/csv/output.csv";


    public static void main (String[] args) {

        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("Query 1 SQL")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        JavaRDD<String> rdd = spark.read().csv(finalPath).toJavaRDD().map(
                row -> row.mkString(",")
        );
        JavaRDD<Tuple4<OffsetDateTime,Double, Double, Double>> rdd2 = Query1Preprocessing.preprocData(rdd);
        System.out.println("Query 1 Spark SQL");

        calculateQuery1SQL(spark, rdd2);
        spark.close();

    }

    private static void calculateQuery1SQL(SparkSession spark, JavaRDD<Tuple4<OffsetDateTime, Double, Double, Double>> rdd) {
        // Register the DataFrame as a SQL temporary view named "query1"
        //creo dataset partendo dai dati che sono coppie javaRDD
        Dataset<Row> df = createSchemaFromPreprocessedData(spark, rdd);
        //df.show();

        //do un nome "query1" alla tabella creata precedentemente
        df.createOrReplaceTempView("query1");

        Dataset<Row> result = spark.sql(
                "SELECT tpep_pickup_datetime, AVG(tip_amount/(total_amount-tolls_amount)) AS avg FROM query1  " +
                        "GROUP BY tpep_pickup_datetime");
        result.createOrReplaceTempView("temp");
        // il risultato di questa query lo chiamo "temp", e da questo momento posso
        // utilizzarlo in un'altra funzione
        result.show();

    }

    private static Dataset<Row> createSchemaFromPreprocessedData(SparkSession spark,
                                                                 JavaRDD<Tuple4<OffsetDateTime, Double, Double, Double>> values){

        // Generate the schema based on the string of schema

        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("tpep_pickup_datetime", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("tip_amount",     DataTypes.DoubleType, true));
        fields.add(DataTypes.createStructField("tolls_amount",         DataTypes.DoubleType, true));
        fields.add(DataTypes.createStructField("total_amount",         DataTypes.DoubleType, true));
        StructType schema = DataTypes.createStructType(fields);

        // Convert records of the RDD to Rows
        JavaRDD<Row> rowRDD = values.map(val -> {
            Timestamp ts = Timestamp.valueOf(val._1().atZoneSameInstant(ZoneId.of("Z")).toLocalDateTime());
            OffsetDateTime date = val._1();
            String month = date.getYear()+ "-"+date.getMonthValue();
            return RowFactory.create(month, val._2(), val._3(),val._4());
        });

        // Apply the schema to the RDD
        // tramite la definizione dei fields dico a spark di creare un dataframe partendo
        // dal java rdd rowRDD
        // questo dataset è interrogabile tramite queries SQL
        Dataset<Row> df = spark.createDataFrame(rowRDD, schema);

        /*
        +--------------------+----------+------------+------------+
        |tpep_pickup_datetime|tip_amount|tolls_amount|total_amount|
        +--------------------+----------+------------+------------+
        | 2021-12-01 00:19:51|       7.6|        6.55|        45.7|
        | 2021-12-01 00:29:07|       0.0|         0.0|        16.8|
        | 2021-12-01 00:42:53|       1.5|         0.0|        14.8|
        | 2021-12-01 00:25:04|      11.1|        6.55|        55.7|
        | 2021-12-01 00:40:04|      6.15|         0.0|       36.45|
        | 2021-12-01 00:05:32|       2.0|         0.0|        18.8|
        +--------------------+----------+------------+------------+


         */

        return df;

    }
}
