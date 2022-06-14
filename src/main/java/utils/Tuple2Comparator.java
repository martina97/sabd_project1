package utils;

import scala.Serializable;
import scala.Tuple2;

import java.util.Comparator;

public class Tuple2Comparator implements Comparator<Tuple2<String,Integer>>, Serializable {

    private static final long serialVersionUID = 1L;
    @Override
    public int compare(Tuple2<String, Integer> v1, Tuple2<String, Integer> v2) {
      
        if (v1._1().compareTo(v2._1()) == 0) {
            return v2._2().compareTo(v1._2());
        }
        return  v2._2().compareTo(v1._2());
    }
}
