import org.apache.spark.api.java.JavaRDD;

import java.util.Arrays;

public class TablesParser {
    public static JavaRDD<String[]> splitAirportsTable(JavaRDD<String> table) {
        //принимаем строку, надо разсплитить..
        /*JavaPairRDD<String, Long> wordsWithCount = splitted.mapToPair(
                s -> new Tuple2<>(s, 1l)
        );
        JavaRDD<String> splitted = distFile.flatMap(Hadoop
                s -> Arrays.stream(Hadoop s.split(Hadoop " ")).iterator(Hadoop )
        );*/
        table.filter(a-> !a.contains("Code"));
        table.map(s -> Arrays.stream(s.split(",(?=\")")));
        table.to
        return table;
    }
}
