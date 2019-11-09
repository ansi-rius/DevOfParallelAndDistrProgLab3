import org.apache.spark.api.java.function.Function2;

public class reduceMethod {
    //Spark RDD reduceByKey function merges the values for each key using an associative reduce function.
    //Reduce Function
    public static Function2<FlightKey, FlightKey, FlightKey> REDUCE = new Function2<FlightKey, FlightKey, FlightKey>() {
        @Override
        public FlightKey call(FlightKey flightKey, FlightKey flightKey2) throws Exception {
            double maxDelay;
            maxDelay = Math.max(flightKey.delay, flightKey2.delay);
            int count = flightKey.counter + flightKey2.counter;
            int canc = flightKey.canceled + flightKey2.canceled;
            int lat = flightKey.late + flightKey2.late;
            FlightKey packedRes = new FlightKey(maxDelay, count, lat, canc);
            return packedRes;
        }
    };
}
