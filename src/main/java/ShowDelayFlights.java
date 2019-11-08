import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class ShowDelayFlights {
    public static void main(String[] args) {
        //инициализация приложения
        SparkConf conf = new SparkConf().setAppName("example");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //загрузка данных
        JavaRDD<String> flightsTable = sc.textFile("/Users/anemone/parallelDevLabs/DevOfParallelAndDistrProgLab3/flights.csv");
        JavaRDD<String> airportsTable = sc.textFile("/Users/anemone/parallelDevLabs/DevOfParallelAndDistrProgLab3/airports.csv");
        //Разбиение строки на слова - splits распарсить..
        JavaRDD<String[]> airports =
                airportsTable.filter(a-> !a.contains("Code"))
                .map(s -> Arrays.stream(s.split(",(?=\")"))
                        .toArray(String[]::new));
        JavaRDD<String[]> flights =
                flightsTable.filter(a-> !a.contains("\"YEAR\""))
                .map(s->Arrays.stream(s.split(","))
                        .toArray(String[]::new));
        //формируем пары <название аеропорта, его код>
        /*JavaPairRDD<String, Long> dictionary =
                dictionaryFile.mapToPair(Hadoop
                        s -> new Tuple2<>(Hadoop s,1l)
                );*/
        JavaPairRDD<String, String> codeNamePairAirport =
                airports.mapToPair(a-> new Tuple2<>(a[0].replace("\"", ""), a[1]) //убрали лишнее
                );

        //делаем задание лабы - связываем тюпл<название, код> с (<код вылета, код прилета>, <делей, кенселед>)
        JavaPairRDD<Tuple2<String,String>, FlightKey> originDestDelayCancelledFlightTuple = TablesParser.makeFlightPair(flights); //?
        //
    }
}

