package query2;

import org.apache.spark.api.java.JavaRDD;
import scala.Tuple3;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class Query2Preprocessing {

    private static final String COLUMN_SEPARATOR = ",";
    private static String pathProva = "/home/martina/Documents/data/csv/provaQuery2.csv";
    private static String pathProvaOutput = "/home/martina/Documents/data/csv/provaQuery2Ordinato.csv";
    private static String finalPath = "/home/martina/Documents/data/csv/output.csv";
    private static String finalPathOutput = "/home/martina/Documents/data/csv/outputFinale.csv";

    public static void main(String[] args) throws Exception {
        InputStream inputStream = new FileInputStream(finalPath);
        List<List<String>> lines = readCsv(inputStream);
        // Create a comparator that compares the elements from column 0,
        // in ascending order
        Comparator<List<String>> c0 = createAscendingComparator(1);
        // Create a comparator that compares primarily by using c0,
        // and secondarily by using c1
        Comparator<List<String>> comparator = createComparator(c0);
        Collections.sort(lines, comparator);
        OutputStream outputStream = new FileOutputStream(finalPathOutput);
        String header = "VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,RatecodeID,store_and_fwd_flag,PULocationID,DOLocationID,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,congestion_surcharge,airport_fee";
        writeCsv(header, lines, outputStream);


    }

    private static void writeCsv(
            String header, List<List<String>> lines, OutputStream outputStream)
            throws IOException
    {
        Writer writer = new OutputStreamWriter(outputStream);
        writer.write(header+"\n");
        for (List<String> list : lines)
        {
            for (int i = 0; i < list.size(); i++)
            {
                writer.write(list.get(i));
                if (i < list.size() - 1)
                {
                    writer.write(COLUMN_SEPARATOR);
                }
            }
            writer.write("\n");
        }
        writer.close();

    }
    private static List<List<String>> readCsv(
            InputStream inputStream) throws IOException
    {
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(inputStream));
        List<List<String>> lines = new ArrayList<List<String>>();

        String line = null;

        // Skip header
        line = reader.readLine();

        while (true)
        {
            line = reader.readLine();
            if (line == null)
            {
                break;
            }
            List<String> list = Arrays.asList(line.split(COLUMN_SEPARATOR));
            lines.add(list);
        }
        return lines;
    }

    @SafeVarargs
    private static <T> Comparator<T>
    createComparator(Comparator<? super T>... delegates)
    {
        return (t0, t1) ->
        {
            for (Comparator<? super T> delegate : delegates)
            {
                int n = delegate.compare(t0, t1);
                if (n != 0)
                {
                    return n;
                }
            }
            return 0;
        };
    }
    private static <T extends Comparable<? super T>> Comparator<List<T>>
    createAscendingComparator(int index)
    {
        return createListAtIndexComparator(Comparator.naturalOrder(), index);
    }
    private static <T> Comparator<List<T>>
    createListAtIndexComparator(Comparator<? super T> delegate, int index)
    {
        return (list0, list1) ->
                delegate.compare(list0.get(index), list1.get(index));
    }

    public static JavaRDD<Tuple3<OffsetDateTime, Double, Double>> preprocData(JavaRDD<String> rdd) {
        // remove header
        String header = rdd.first();

        return rdd.filter(x -> !(x.contains(header) & !(x.contains("NaN")))).map(
                row -> {
                    String[] myFields = row.split(",");
                    //System.out.println("tip == " + myFields[14] + "toll == " + myFields[15] + "tot == " +myFields[17]);
                    System.out.println("tpep_pickup_datetime == " + myFields[1]);
                    //System.out.println(myFields[1].getClass());
                    DateTimeFormatter f = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
                    //OffsetDateTime odt = OffsetDateTime.parse( "2012-10-01T09:45:00.000+02:00" );
                    OffsetDateTime odt = OffsetDateTime.parse( myFields[1]);

                    System.out.println("odt == " + odt);

                    //LocalDate ld = LocalDate.parse( myFields[1] , f ) ;
                    //System.out.println("ld == " + ld);
                    OffsetDateTime tpep_pickup_datetime = odt;
                    System.out.println("tpep_pickup_datetime == " + odt);
                    System.out.println(tpep_pickup_datetime.getClass());
                    Double tip_amount = Double.valueOf(myFields[14]);
                    Double payment_type = Double.valueOf(myFields[16]);
                    //if ( !(Double.isNaN(tip)) & !(Double.isNaN(toll)) & !(Double.isNaN(tot))) {
                    return new Tuple3<>(tpep_pickup_datetime, tip_amount, payment_type);
                    // }

                }
        );
    }
}
