package utilities;

import scala.Tuple2;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

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
}
