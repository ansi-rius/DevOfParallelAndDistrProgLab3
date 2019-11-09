import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;

public class TablesParser {
    public static JavaPairRDD<Tuple2<String,String>, FlightKey> makeFlightPair(JavaRDD<String[]> flights) {
        /*JavaPairRDD<String, Long> dictionary =
                dictionaryFile.mapToPair(Hadoop
                        s -> new Tuple2<>(Hadoop s,1l)
                );*/
        //Требуется определить для пары <аэропорт отлета, аэропорт прибытия>
        // s[11] s[14] - tuple2?
        return flights.mapToPair(s->new Tuple2<>(new Tuple2<>(s[11], s[14]), new FlightKey(s[18], s[19])));
            //s[11] - origin, s[14] - dest
            //s[18] - arrDelayNew s[19] - cancelled

        //return flights;
    }
    //Spark RDD reduceByKey function merges the values for each key using an associative reduce function.
    //Reduce Function
    public static Function2<FlightKey, FlightKey, FlightKey> reduce = new Function2<FlightKey, FlightKey, FlightKey>() {
        @Override
        public FlightKey call(FlightKey flightKey, FlightKey flightKey2) throws Exception {
            double maxDelay;
            //maxDelay = Math.max(flightKey.delay, flightKey2.delay);
            
            int count = flightKey.counter + flightKey2.counter;
            int canc = flightKey.canceled + flightKey2.canceled;
            int lat = flightKey.late + flightKey2.late;
            FlightKey packedRes = new FlightKey(maxDelay, count, lat, canc);
            return null;
        }
    };
}
