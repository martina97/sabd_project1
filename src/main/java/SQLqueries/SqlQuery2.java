package SQLqueries;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple3;
import utils.QueriesPreprocessing;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;


public class SqlQuery2 {
    public static void query2SQLMain(JavaRDD<String> rdd, SparkSession spark) {
        JavaRDD<Tuple3<LocalDateTime, Double, Double>> rddPreproc = QueriesPreprocessing.Query2Preprocessing(rdd).cache();
        System.out.println("Query 2 Spark SQL");
        //calculateQuery2SQL(spark, rddPreproc);

        // Register the DataFrame as a SQL temporary view named "query1"
        //creo dataset partendo dai dati che sono coppie javaRDD
        Dataset<Row> df = createSchemaFromPreprocessedDataQ2(spark, rddPreproc);

        //do un nome "query1" alla tabella creata precedentemente
        df.createOrReplaceTempView("query2");
        // todo: vedere codice prof per piu query su stesso df
        df.cache();

        // --------  Calcolo Distribution of the number of trips distrNumbTripPerH  --------

        Dataset<Row> tripDistribution = calculateDistributionTrip(df, spark);
        tripDistribution.createOrReplaceTempView("tripDistribution");


        //--------   Calcolo average tip and its standard deviation --------
        Dataset<Row> avgStdvTip = calculateAvgStdevTip(df, spark);
        avgStdvTip.createOrReplaceTempView("avgStdvTip");





        /*
        Dataset<Row>   resultQ2 = spark.sql("SELECT avgStdvTip.tpep_pickup_datetime, average, distribution " +
                "FROM avgStdvTip JOIN tripDistribution ON avgStdvTip.tpep_pickup_datetime = tripDistribution.tpep_pickup_datetime " +
                "ORDER BY avgStdvTip.tpep_pickup_datetime ASC");

         */


        /* FUNZIONA CON tripDistribution E avgStdvTip
        Dataset<Row>   resultQ2 = spark.sql("SELECT tripDistribution.tpep_pickup_datetime, tripDistribution.distribution, average, std " +
                "FROM tripDistribution JOIN avgStdvTip ON avgStdvTip.tpep_pickup_datetime = tripDistribution.tpep_pickup_datetime " +
                "ORDER BY tripDistribution.tpep_pickup_datetime ASC");

         */





        // -------- Calcolo most popular payment method --------
        Dataset<Row> topPaym = calculateTopPaymentMethod(df, spark);
        topPaym.createOrReplaceTempView("topPaym");




        Dataset<Row>   resultQ = spark.sql(
                "select tripDistribution.tpep_pickup_datetime, tripDistribution.distribution,avgStdvTip.average, avgStdvTip.std, topPaym.payment_type, topPaym.occurences from tripDistribution " +
                        "join avgStdvTip on avgStdvTip.tpep_pickup_datetime = tripDistribution.tpep_pickup_datetime " +
                        "join topPaym on topPaym.tpep_pickup_datetime = tripDistribution.tpep_pickup_datetime"
        );
        resultQ.show();



        /*
        // todo: scommentare
        Dataset<Row>   resultQ = spark.sql(
                "select tripDistribution.tpep_pickup_datetime, tripDistribution.distribution, avgStdvTip.average from tripDistribution " +
                        "join avgStdvTip on avgStdvTip.tpep_pickup_datetime = tripDistribution.tpep_pickup_datetime "
        );
        resultQ.show();

         */



        /*
        Dataset<Row>   resultQ2 = spark.sql("SELECT tripDistribution.tpep_pickup_datetime, tripDistribution.distribution, average, std , payment_type, occurences " +
                "FROM tripDistribution JOIN avgStdvTip ON avgStdvTip.tpep_pickup_datetime = tripDistribution.tpep_pickup_datetime " +
                "JOIN topPaym ON avgStdvTip.tpep_pickup_datetime = topPaym.tpep_pickup_datetime " +
                "ORDER BY tripDistribution.tpep_pickup_datetime ASC");

         */


        /*
        Dataset<Row> resultQ2 = spark.sql(
          " SELECT * from tripDistribution " +
                  " INNER JOIN (" +
                  "SELECT average, std from avgStdvTip where tripDistribution.tpep_pickup_datetime = avgStdvTip.tpep_pickup_datetime)"
        );

         */





    }

    private static Dataset<Row> calculateTopPaymentMethod(Dataset<Row> df, SparkSession spark) {
        System.out.println("\n\n ---------- calculateTopPaymentMethod ---------- ");
        Dataset<Row> result = spark.sql(
                "SELECT tpep_pickup_datetime, payment_type, count(payment_type) as occurences FROM query2  " +
                        "GROUP BY tpep_pickup_datetime, payment_type order by tpep_pickup_datetime");
        /*
        "SELECT tpep_pickup_datetime, payment_type,  count(payment_type) AS occurences FROM query2  " +
                        "GROUP BY tpep_pickup_datetime order by tpep_pickup_datetime");
         */

        result.createOrReplaceTempView("temp3");
        result.show();

        /*
        Dataset<Row> result2 = spark.sql(
                "SELECT tpep_pickup_datetime, max(occurences) as max_occurences FROM temp  " +
                        "GROUP BY tpep_pickup_datetime, payment_type order by tpep_pickup_datetime");

         */
        /* todo: ---- FUNZIONAAAA -----
        Dataset<Row> result2 = spark.sql(
                " SELECT * from temp3 "+
                "INNER JOIN ( " +
                        "SELECT tpep_pickup_datetime, max(occurences) as maxCount FROM temp3 GROUP BY tpep_pickup_datetime) prova2 " +
                        "on temp3.tpep_pickup_datetime = prova2.tpep_pickup_datetime and temp3.occurences = prova2.maxCount" +
                        " order by temp3.tpep_pickup_datetime"
        );

         */
        Dataset<Row> result2 = spark.sql(
                " SELECT * from temp3 prova"+
                        " where occurences = ( select max(occurences) from temp3 where tpep_pickup_datetime = prova.tpep_pickup_datetime ) " +
                        " order by tpep_pickup_datetime"
        );

        result2.show();
        // il risultato di questa query lo chiamo "temp", e da questo momento posso
        // utilizzarlo in un'altra funzione

        return result2;
    }

    private static Dataset<Row> calculateAvgStdevTip(Dataset<Row> df, SparkSession spark) {
        System.out.println("\n\n ---------- calculateAvgStdevTip ---------- ");
        Dataset<Row> result = spark.sql(
                "SELECT tpep_pickup_datetime, avg(tip_amount) AS average, std(tip_amount)  AS std FROM query2  " +
                        "GROUP BY tpep_pickup_datetime order by tpep_pickup_datetime");

        // il risultato di questa query lo chiamo "temp", e da questo momento posso
        // utilizzarlo in un'altra funzione
        result.show();
        return result;
    }

    private static Dataset<Row> calculateDistributionTrip(Dataset<Row> df, SparkSession spark) {
        System.out.println("\n\n ---------- calculateDistributionTrip ---------- ");
        Dataset<Row> result = spark.sql(
                "SELECT tpep_pickup_datetime, count(tpep_pickup_datetime) AS distribution FROM query2  " +
                        "GROUP BY tpep_pickup_datetime order by tpep_pickup_datetime");


        // il risultato di questa query lo chiamo "temp", e da questo momento posso
        // utilizzarlo in un'altra funzione
        result.show();
        return result;
    }


    private static Dataset<Row> createSchemaFromPreprocessedDataQ2(SparkSession spark, JavaRDD<Tuple3<LocalDateTime, Double, Double>> rdd) {
        // Generate the schema based on the string of schema

        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("tpep_pickup_datetime", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("payment_type",     DataTypes.DoubleType, true));
        fields.add(DataTypes.createStructField("tip_amount",         DataTypes.DoubleType, true));
        StructType schema = DataTypes.createStructType(fields);

        // Convert records of the RDD to Rows
        JavaRDD<Row> rowRDD = rdd.map(val -> {
            LocalDateTime date = val._1();
            return RowFactory.create(date.getHour(), val._2(), val._3());
        });

        // Apply the schema to the RDD
        // tramite la definizione dei fields dico a spark di creare un dataframe partendo
        // dal java rdd rowRDD
        // questo dataset è interrogabile tramite queries SQL.
        Dataset<Row> df = spark.createDataFrame(rowRDD, schema);
        //df.show();

        return df;
    }

}
