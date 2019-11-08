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
        JavaRDD<String> flights = sc.textFile("/Users/anemone/parallelDevLabs/DevOfParallelAndDistrProgLab3/flights.csv");
        JavaRDD<String> airports = sc.textFile("/Users/anemone/parallelDevLabs/DevOfParallelAndDistrProgLab3/airports.csv");

        
    }
}

