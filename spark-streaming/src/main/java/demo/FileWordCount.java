package demo;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class FileWordCount {

    public static void main(String[] args) throws Exception {

        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("demo.FileWordCount");

        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        JavaDStream<String> lines = streamingContext.textFileStream("/Users/luguanxing/data/spark-test");

        JavaPairDStream<String, Integer> rdds =
                lines.flatMap(
                        line -> {
                            String[] words = line.split(" ");
                            Iterator<String> iterator = Arrays.asList(words).iterator();
                            return iterator;
                        }
                ).mapToPair(
                        word -> {
                            return new Tuple2<>(word, 1);
                        }
                ).reduceByKey(
                        (count1, count2) -> {
                            return count1 + count2;
                        }
                );

        rdds.print();

        streamingContext.start();
        streamingContext.awaitTermination();

    }

}
