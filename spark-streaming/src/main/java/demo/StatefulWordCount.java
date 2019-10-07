package demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class StatefulWordCount {

    public static void main(String[] args) throws Exception {

        SparkConf sparkConf = new SparkConf().setAppName("demo.StatefulWordCount").setMaster("local[2]");

        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        JavaReceiverInputDStream<String> lines = streamingContext.socketTextStream("127.0.0.1", 12345);

        // 设置状态检查点保存目录,生产环境应保存到hdfs中
        streamingContext.checkpoint("./state");

        JavaPairDStream<String, Integer> rdds =
                lines.flatMap(
                        line -> {
                            String[] words = line.split(" ");
                            Iterator<String> iterator = Arrays.asList(words).iterator();
                            return iterator;
                        }
                ).mapToPair(
                        word -> new Tuple2<>(word, 1)
                ).updateStateByKey(
                        (currentValues, state) -> {
                            // 如果该key出现过即state.isPresent()则取，否则得到0
                            Integer newValue = state.orElse(0);
                            // 取到的值加上最新的所有值
                            for (Integer currentValue : currentValues) {
                                newValue += currentValue;
                            }
                            return Optional.of(newValue);
                        }
                );

        rdds.print();

        streamingContext.start();
        streamingContext.awaitTermination();

    }

}
