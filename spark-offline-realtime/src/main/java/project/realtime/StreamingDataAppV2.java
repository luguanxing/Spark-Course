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
import scala.Tuple4;
import project.utils.RedisUtil;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class StreamingDataAppV2 {

    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("KafkaStreamingAppV2");

        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(10));

        // 对接kafka流
        Map kafkaParams = new HashMap();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.kafkaBrokerList);
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, AppConfig.kafkaGroupId);
        kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "latest");
        Set topics = new HashSet();
        topics.add(AppConfig.kafkaTopic);
        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(streamingContext, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

        // 获取json数据流
        JavaDStream<String> jsonStream = messages.map(pair -> pair._2);

        // 处理数据流
        jsonStream.foreachRDD(
                rdd -> {
                    JavaRDD<Map> dataRdd = rdd.map(json -> new Gson().fromJson(json, Map.class));

                    // 获取最小粒度的rdd
                    JavaRDD<Tuple4<String, String, String, Tuple2<Integer, Integer>>> minDataRdd = dataRdd.map(
                            data -> {
                                String time = data.get("time").toString();
                                int payed = ((Double) Double.parseDouble(data.get("payed").toString())).intValue();
                                int fee = ((Double) Double.parseDouble(data.get("fee").toString())).intValue();
                                // 未付款，则金额为0
                                fee = payed == 1 ? fee : 0;

                                String day = time.substring(0, 10);
                                String hour = time.substring(0, 13);
                                String minute = time.substring(0, 16);
                                return new Tuple4<String, String, String, Tuple2<Integer, Integer>>(day, hour, minute, new Tuple2(payed, fee));
                            }
                    );

                    // 按天粒度，其它粒度同理
                    minDataRdd.mapToPair(
                            tuple4 -> {
                                String day = tuple4._1();
                                Tuple2<Integer, Integer> cntSum = tuple4._4();
                                return new Tuple2<>(day, cntSum);
                            }
                    ).reduceByKey(
                            (tuple2_1, tuple2_2) -> {
                                Integer cnt1 = tuple2_1._1;
                                Integer cnt2 = tuple2_2._1;
                                Integer sum1 = tuple2_1._2;
                                Integer sum2 = tuple2_2._2;
                                return new Tuple2<>(cnt1+cnt2, sum1+sum2);
                            }
                    ).foreachPartition(
                            partition -> {
                                Jedis jedis = RedisUtil.getJedis();
                                while (partition.hasNext()) {
                                    Tuple2<String, Tuple2<Integer, Integer>> tuple2 = partition.next();
                                    String day = tuple2._1;
                                    Tuple2<Integer, Integer> cntSum = tuple2._2;
                                    Integer cnt = cntSum._1;
                                    Integer sum = cntSum._2;
                                    jedis.incrBy("PayedCount-" + day, cnt);
                                    jedis.incrBy("PayedSum-" + day, sum);
                                    System.err.println(day + " ==[cnt]==> " + cnt);
                                    System.err.println(day + " ==[sum]==> " + sum);
                                }
                                jedis.close();
                            }
                    );
                }
        );

        streamingContext.start();
        streamingContext.awaitTermination();
    }

}
