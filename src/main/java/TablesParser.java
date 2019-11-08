import org.apache.spark.api.java.JavaRDD;

public class TablesParser {
    public static JavaRDD<String[]> splitAirportsTable(JavaRDD<String> table) {
        //принимаем строку, надо разсплитить..
        /*JavaPairRDD<String, Long> wordsWithCount = splitted.mapToPair(
                s -> new Tuple2<>(s, 1l)
        );*/
        .map
        return table;
    }
}
