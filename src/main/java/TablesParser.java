import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class TablesParser {

    public static JavaRDD<String[]> splitAirportsTable(JavaRDD<String> airportsTable) {
        return airportsTable.filter(a-> !a.contains("Code"))
                .map(s -> Arrays.stream(s.split(",(?=\")"))
                        .toArray(String[]::new));

    }

    public static JavaRDD<String[]> splitFlightsTable(JavaRDD<String> flightsTable) {
        return flightsTable.filter(a-> !a.contains("\"YEAR\""))
                .map(s->Arrays.stream(s.split(","))
                        .toArray(String[]::new));
    }

    public static JavaPairRDD<String, String> makeAirportPairs(JavaRDD<String[]> airports) {
        return airports.mapToPair(a-> new Tuple2<>(a[0].replace("\"", ""), a[1]) //убрали лишнее
        );
    }

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

    public static JavaPairRDD<Tuple2<String, String>, List<String>> writeRes(JavaPairRDD<Tuple2<String, String>, FlightKey> reduceData) {
        return reduceData.mapToPair(
                s->new Tuple2<>(s._1, //забираем пару код_дест, код_ориджин
                        Arrays.asList(String.valueOf(s._2.delay), //забрали макс делей
                                String.format("%.2f %%",((double)s._2.late/s._2.counter*100)), //% Of late
                                String.format("%.2f %%",((double)s._2.canceled/s._2.counter*100))
                        )));
    }

}
