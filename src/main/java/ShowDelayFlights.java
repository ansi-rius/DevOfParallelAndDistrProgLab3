import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Array;
import scala.Tuple2;

import java.util.List;
import java.util.Map;

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
        //коллект эз мап — для связывания с таблицей аэропортов — предварительно выкачиваем список
        //аэропортов в главную функцию с помощью метода collectAsMap
        // collectAsMap - Collect the result as a map to provide easy lookup
        Map<String, String> airMap = codeNamePairAirport.collectAsMap();
        //создаем в основном методе main переменную broadcast сюда кидаем пары код, имя аэропорта
        final Broadcast<Map<String, String>> airportsBroadcasted =
                sc.broadcast(airMap);
        //c помощью функции reduce или аналогичных расчитываем максимальное
        //время опоздания, процент опоздавших+отмененных рейсов
        JavaPairRDD<Tuple2<String, String>, FlightKey> reduceData = originDestDelayCancelledFlightTuple.reduceByKey(TablesParser.reduce);
        //формируем строки для результата... res должен быть:
        // name_origin, name_dest, maxDelay, %OfLate, %OfCanceled
        //связать вывод с именами аэропортов
        //обогащаем его именами аэропортов, обращаясь внутри
        //функций к объекту airportsBroadcasted.value()
        JavaPairRDD<Tuple2<String, String>, List<String>> res =
                reduceData.mapToPair(
                        s->new Tuple2<>(s._1, //забираем пару код_дест, код_ориджин
                                Arrays.asList(String.valueOf(s._2.delay)), //забрали макс делей
                                ((double)(s._2.late/s._2.counter*100)), //%Oflate
                                String.valueOf((double)(s._2.canceled/s._2.counter*100)));
                )

                /*JavaPairRDD<String, Long> dictionary =
                dictionaryFile.mapToPair(Hadoop
                        s -> new Tuple2<>(Hadoop s,1l)
                );*/
    }
}

