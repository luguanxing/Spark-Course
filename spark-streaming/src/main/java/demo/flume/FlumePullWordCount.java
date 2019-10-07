package demo.flume;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;
import scala.Tuple2;

import java.util.Arrays;

public class FlumePullWordCount {

    public static void main(String[] args) throws Exception {

        SparkConf sparkConf = new SparkConf(); //.setMaster("local[2]").setAppName("FlumePullWordCount");

        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        // 整合flume
        JavaReceiverInputDStream<SparkFlumeEvent> flumeDStream = FlumeUtils.createPollingStream(streamingContext, "127.0.0.1", 54321);

        flumeDStream.map(
                flumeMsg -> new String(flumeMsg.event().getBody().array()).trim()
        ).flatMap(
                line -> {
                    String[] words = line.split(" ");
                    return Arrays.asList(words).iterator();
                }
        ).mapToPair(
                word -> new Tuple2<>(word, 1)
        ).reduceByKey(
                (count1, count2) -> (count1 + count2)
        ).print();

        streamingContext.start();

        streamingContext.awaitTermination();

    }

}
