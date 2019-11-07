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
        JavaRDD<String> distFile = sc.textFile("war-and-peace-1.txt");
        //разбиение строки на слова
        JavaRDD<String> splitted = distFile.flatMap(
                s -> Arrays.stream(s.split(" ")).iterator()
        );
        //отображение слов в пару <Слово, 1>
        JavaPairRDD<String, Long> wordsWithCount = splitted.mapToPair(
                s -> new Tuple2<>(s,1)
        );
        //считаем одинаковые слова
        JavaPairRDD<String, Long>
    }
}

