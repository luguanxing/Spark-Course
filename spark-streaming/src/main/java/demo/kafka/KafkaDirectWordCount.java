package demo.kafka;

import kafka.serializer.StringDecoder;
import org.apache.kafka.clients.consumer.ConsumerConfig;;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

public class KafkaDirectWordCount {

    public static void main(String[] args) throws Exception {

        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("KafkaDirectWordCount");

        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        // 对接kafka
        Map kafkaParams = new HashMap();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "test_group");
        Set topics = new HashSet();
        topics.add("test");
        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(streamingContext, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

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
