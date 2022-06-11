package SQLqueries;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Tuple5;
import utils.QueriesPreprocessing;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SqlQuery1 {
    //private static String finalPath = "prova_2021_12.csv";
    public static String parquetFile1 = "./docker/data/yellow_tripdata_2021-12.parquet";

    //private static String finalPath = "/home/martina/Documents/data/csv/output.csv";


    //public static void main (String[] args) {
    public static void query1SQLMain(JavaRDD<String> rdd, SparkSession spark) {


        JavaRDD<Tuple5<LocalDateTime, Double, Double, Double, Double>> rddPreproc = QueriesPreprocessing.Query1Preprocessing(rdd);
        System.out.println("Query 1 Spark SQL");

        calculateQuery1SQL(spark, rddPreproc);



    }

    private static void calculateQuery1SQL(SparkSession spark, JavaRDD<Tuple5<LocalDateTime, Double, Double, Double, Double>> rdd) {
        // Register the DataFrame as a SQL temporary view named "query1"
        //creo dataset partendo dai dati che sono coppie javaRDD
        Dataset<Row> df = createSchemaFromPreprocessedData(spark, rdd);
        //df.show();

        //do un nome "query1" alla tabella creata precedentemente
        df.createOrReplaceTempView("query1");

        Dataset<Row> result = spark.sql(
                "SELECT tpep_pickup_datetime, AVG(tip_amount/(total_amount-tolls_amount)) AS tip_percentage, count(*) as trips_number FROM query1  " +
                        "GROUP BY tpep_pickup_datetime");
        result.createOrReplaceTempView("temp");
        // il risultato di questa query lo chiamo "temp", e da questo momento posso
        // utilizzarlo in un'altra funzione
        result.show();

        
       //CsvWriter.writeQuery1SQL(result);
        //result.write().format("csv").save("./results/query1SQL.csv");
       /*
        result.foreach(
               (ForeachFunction<Row>) row -> System.out.println( " row.get(0) == " + row.get(0) + "row.get(1) == " + row.get(1))
       );

        */

         
    }

    private static Dataset<Row> createSchemaFromPreprocessedData(SparkSession spark,
                                                                 JavaRDD<Tuple5<LocalDateTime, Double, Double, Double, Double>> values){

        // Generate the schema based on the string of schema

        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("tpep_pickup_datetime", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("payment_type", DataTypes.DoubleType, true));
        fields.add(DataTypes.createStructField("tip_amount",     DataTypes.DoubleType, true));
        fields.add(DataTypes.createStructField("tolls_amount",         DataTypes.DoubleType, true));
        fields.add(DataTypes.createStructField("total_amount",         DataTypes.DoubleType, true));
        StructType schema = DataTypes.createStructType(fields);

        // Convert records of the RDD to Rows
        JavaRDD<Row> rowRDD = values.map(val -> {
            LocalDateTime date = val._1();
            String month = date.getYear()+ "-"+date.getMonthValue();

            return RowFactory.create(month, val._2(), val._3(),val._4(), val._5());
        });

        // Apply the schema to the RDD
        // tramite la definizione dei fields dico a spark di creare un dataframe partendo
        // dal java rdd rowRDD
        // questo dataset è interrogabile tramite queries SQL.
        Dataset<Row> df = spark.createDataFrame(rowRDD, schema);
        df.show();

        /*
        +--------------------+------------+----------+------------+------------+
        |tpep_pickup_datetime|payment_type|tip_amount|tolls_amount|total_amount|
        +--------------------+------------+----------+------------+------------+
        |             2021-12|         1.0|       7.6|        6.55|        45.7|
        |             2021-12|         1.0|       2.0|         0.0|        19.3|
        |             2021-12|         1.0|      2.05|         0.0|       12.35|
        |             2021-12|         1.0|      5.42|        6.55|       41.52|
        |             2021-12|         1.0|       1.0|         0.0|        14.8|
        |             2021-12|         1.0|      3.35|         0.0|       14.65|
        |             2021-12|         1.0|      2.86|         0.0|       17.16|
        |              2022-1|         1.0|      2.25|         0.0|       18.05|
        |              2022-1|         1.0|      3.06|         0.0|       18.36|
        +--------------------+------------+----------+------------+------------+
         */

        return df;

    }
}
