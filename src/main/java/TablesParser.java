import org.apache.spark.api.java.JavaRDD;

public class TablesParser {
    public static JavaRDD<String[]> splitAirportsTable(JavaRDD<String> table) {
        //принимаем строку, надо разсплитить..
        /*JavaPairRDD<String, Long> wordsWithCount = splitted.mapToPair(
                s -> new Tuple2<>(s, 1l)
        );
        JavaRDD<String> splitted = distFile.flatMap(Hadoop
                s -> Arrays.stream(Hadoop s.split(Hadoop " ")).iterator(Hadoop )
        );*/
        .filter(a-> !a.contsins(""));
        return table;
    }
}
