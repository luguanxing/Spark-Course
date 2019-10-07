package demo.kafka;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class KafkaReceiverWordCount {

    public static void main(String[] args) throws Exception {

        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("KafkaReceiverWordCount");

        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        // 对接kafka
        String zkHost = "localhost:2181";
        String group = "test_group";
        Map topicMap = new HashMap();
        topicMap.put("test", 1);
        JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(streamingContext, zkHost, group, topicMap);

        // 处理流
        messages.map(
                pair -> pair._2
        ).flatMap(
                line -> Arrays.asList(line.split(" ")).iterator()
        ).mapToPair(
                word -> new Tuple2<>(word, 1)
        ).reduceByKey(
                (count1, count2) -> (count1 + count2)
        ).print();

        streamingContext.start();
        streamingContext.awaitTermination();

    }

}
