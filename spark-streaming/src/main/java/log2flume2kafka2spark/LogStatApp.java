package log2flume2kafka2spark;

import kafka.serializer.StringDecoder;
import log2flume2kafka2spark.dao.ClickCountDAO;
import log2flume2kafka2spark.domain.ClickLog;
import log2flume2kafka2spark.domain.ClickLogRowKey;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.sources.In;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.*;

public class LogStatApp {

    private static SimpleDateFormat parseSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private static SimpleDateFormat formatSdf = new SimpleDateFormat("yyyyMMdd");

    public static void main(String[] args) throws Exception {

        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("LogStatApp");

        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(10));

        // 对接kafka
        Map kafkaParams = new HashMap();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "log_stat");
        Set topics = new HashSet();
        topics.add("test");

        // 获取kafka流
        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(streamingContext, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

        // 处理流，进行清洗
        JavaDStream<ClickLog> filterLogData = messages.map(pair -> pair._2)
                .map(
                        line -> {
                            // 解析数据
                            String[] infos = line.split("\t");
                            String ip = infos[0];
                            Long ts = parseTime(infos[1]);
                            String url = infos[2].split(" ")[1];
                            Integer statusCode = Integer.parseInt(infos[3]);
                            String referer = infos[4];
                            Integer courseId = 0;
                            if (url.startsWith("/class")) {
                                String courseIdHtml = url.split("/")[2];
                                courseId = Integer.parseInt(courseIdHtml.split("\\.")[0]);
                            }
                            // 封装数据
                            ClickLog clickLog = new ClickLog(ip, ts, courseId, statusCode, referer);
                            return clickLog;
                        }
                ).filter(
                        clickLog -> {
                            if (clickLog.getCourseId() == 0) {
                                return false;
                            } else {
                                return true;
                            }
                        }
                );

        // 统计今天到现在的课程访问量
        JavaPairDStream<String, Long> rdds = filterLogData.mapToPair(
                clickLog -> {
                    String rowkey = formatSdf.format(clickLog.getTs()) + "_" + clickLog.getCourseId();
                    return new Tuple2(rowkey, 1l);
                }
        ).reduceByKey(
                (count1, count2) -> Long.parseLong(count1.toString()) + Long.parseLong(count2.toString())
        );

        // 数据写到hbase中
        rdds.foreachRDD(
                rdd -> rdd.foreachPartition(
                        partitionOfRecords -> {
                            List<ClickLogRowKey> list = new ArrayList<>();
                            while (partitionOfRecords.hasNext()) {
                                Tuple2<String, Long> tuple2 = partitionOfRecords.next();
                                String rowkey = tuple2._1;
                                Long value = tuple2._2;
                                list.add(new ClickLogRowKey(rowkey, value));
                            }
                            ClickCountDAO.save(list);
                        }
                )
        );

        streamingContext.start();
        streamingContext.awaitTermination();
    }

    public static Long parseTime(String date) {
        long ts = 0;
        try {
            ts = parseSdf.parse(date).getTime();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ts;
    }

}
