package demo;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class WindowWordCount {


    public static void main(String[] args) throws Exception {

        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("demo.WindowWordCount");

        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        JavaReceiverInputDStream<String> lines = streamingContext.socketTextStream("127.0.0.1", 12345);

        JavaPairDStream<String, Integer> rdds =
        lines.flatMap(
                line -> {
                    String[] words = line.split(" ");
                    Iterator<String> iterator = Arrays.asList(words).iterator();
                    return iterator;
                }
        ).mapToPair(
                word -> new Tuple2<>(word, 1)
        ).reduceByKeyAndWindow(
                (count1, count2) ->  (count1 + count2), Durations.seconds(15), Durations.seconds(5)
        );

        rdds.print();

        streamingContext.start();
        streamingContext.awaitTermination();

    }

}
