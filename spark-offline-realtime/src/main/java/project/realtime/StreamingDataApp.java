package project.realtime;

import com.google.gson.Gson;
import kafka.serializer.StringDecoder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import redis.clients.jedis.Jedis;
import scala.Tuple2;
import project.utils.RedisUtil;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class StreamingDataApp {

    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("KafkaStreamingApp");

        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        // 对接kafka流
        Map kafkaParams = new HashMap();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.kafkaBrokerList);
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, AppConfig.kafkaGroupId);
        Set topics = new HashSet();
        topics.add(AppConfig.kafkaTopic);
        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(streamingContext, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

        // 获取json数据流
        JavaDStream<String> jsonStream = messages.map(pair -> pair._2);

        // 处理数据流
        jsonStream.foreachRDD(
                rdd -> {
                    JavaRDD<Map> dataRdd = rdd.map(json -> new Gson().fromJson(json, Map.class));

                    // 缓存多次使用的数据
                    dataRdd.cache();

                    // 按天获取付款数
                    dataRdd.mapToPair(
                            data -> {
                                String time = data.get("time").toString();
                                String day = time.substring(0, 10);
                                int payed = ((Double) Double.parseDouble(data.get("payed").toString())).intValue();
                                return new Tuple2<>(day, payed);
                            }
                    ).reduceByKey(
                            (payedCnt1, payedCnt2) -> payedCnt1 + payedCnt2
                    ).foreachPartition(
                            partition -> {
                                Jedis jedis = RedisUtil.getJedis();
                                while (partition.hasNext()) {
                                    Tuple2<String, Integer> tuple2 = partition.next();
                                    String day = tuple2._1;
                                    Integer payedCnt = tuple2._2;
                                    jedis.incrBy("PayedCount-" + day, payedCnt);
                                    System.err.println(day + " ==[cnt]==> " + payedCnt);
                                }
                                jedis.close();
                            }
                    );

                    // 按天获取付款总额数
                    dataRdd.mapToPair(
                            data -> {
                                String time = data.get("time").toString();
                                String day = time.substring(0, 10);
                                int payed = ((Double) Double.parseDouble(data.get("payed").toString())).intValue();
                                int fee = ((Double) Double.parseDouble(data.get("fee").toString())).intValue();
                                return new Tuple2<>(day, payed == 1 ? fee : 0);
                            }
                    ).reduceByKey(
                            (fee1, fee2) -> fee1 + fee2
                    ).foreachPartition(
                            partition -> {
                                Jedis jedis = RedisUtil.getJedis();
                                while (partition.hasNext()) {
                                    Tuple2<String, Integer> tuple2 = partition.next();
                                    String day = tuple2._1;
                                    Integer feeSum = tuple2._2;
                                    jedis.incrBy("PayedSum-" + day, feeSum);
                                    System.err.println(day + " ==[sum]==> " + feeSum);
                                }
                                jedis.close();
                            }
                    );

                    // 解除缓存
                    dataRdd.unpersist(true);
                }
        );

        streamingContext.start();
        streamingContext.awaitTermination();
    }

}
