import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.Arrays;

public class TablesParser {
    public static JavaPairRDD<Tuple2<String,String>, FlightKey> makeFlightPair(JavaRDD<String[]> flights) {
        /*JavaPairRDD<String, Long> dictionary =
                dictionaryFile.mapToPair(Hadoop
                        s -> new Tuple2<>(Hadoop s,1l)
                );*/
        flights.mapToPair(s->new Tuple2<> ()) //s[11] - origin, 
        return flights;
    }
}
