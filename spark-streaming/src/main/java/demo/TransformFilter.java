package demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class TransformFilter {

    /**
     * 功能：通过transoform和关联实现黑名单过滤
     */
    public static void main(String[] args) throws Exception {

        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("demo.TransformFilter");

        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        JavaReceiverInputDStream<String> lines = streamingContext.socketTextStream("127.0.0.1", 12345);

        // 模拟黑名单
        List<String> blackList = new ArrayList<>();
        blackList.add("van");
        blackList.add("ddf");
        JavaPairRDD<String, Boolean> blackRDD = streamingContext.sparkContext().parallelize(blackList).mapToPair(word -> new Tuple2<>(word, true));

        // 格式化原始数据 (van,1) -> van,(van,1)
        lines.mapToPair(
                line -> {
                    String name = line.split(",")[0];
                    return new Tuple2(name, line);
                }
        ).transform(
                // 过滤黑名单数据 leftOuterJoin : van , ((van,1), true)
                (Function<JavaPairRDD<String, String>, JavaRDD<String>>) rdd ->
                        rdd.leftOuterJoin(blackRDD)
                                .filter(
                                        tuple2 -> {
                                            //  判断 van , ((van,1), true)
                                            Boolean isBlocked = tuple2._2._2.orElse(false);
                                            if (isBlocked) {
                                                System.out.println("blocked:" + tuple2);
                                                return false;
                                            } else {
                                                return true;
                                            }
                                        }
                                )
                                .map(tuple2 -> tuple2._2._1)
        ).print();

        streamingContext.start();
        streamingContext.awaitTermination();

    }

}
