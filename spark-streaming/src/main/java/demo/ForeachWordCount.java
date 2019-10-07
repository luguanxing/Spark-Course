package demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Arrays;
import java.util.Iterator;

public class ForeachWordCount {

    public static void main(String[] args) throws Exception {

        SparkConf sparkConf = new SparkConf().setAppName("demo.ForeachWordCount").setMaster("local[2]");

        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        JavaReceiverInputDStream<String> lines = streamingContext.socketTextStream("127.0.0.1", 12345);

        // 设置状态检查点保存目录,生产环境应保存到hdfs中
        streamingContext.checkpoint("./state");

        JavaPairDStream<String, Integer> rdds =
                lines.flatMap(
                        line -> Arrays.asList(line.split(" ")).iterator()
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

        rdds.foreachRDD(
                rdd -> rdd.foreachPartition(
                        partitionOfRecords -> {
                            Connection connection = createConnecttion();
                            while (partitionOfRecords.hasNext()) {
                                Tuple2<String, Integer> tuple2 = partitionOfRecords.next();
                                String word = tuple2._1;
                                Integer wordcount = tuple2._2;
                                // 将结果写入mysql
                                String sql = "INSERT INTO tbl_wordcount(word, wordcount) VALUES ('" + word + "', '" + wordcount + "')";
                                connection.createStatement().execute(sql);
                            }
                            connection.close();
                        }
                )
        );

        streamingContext.start();
        streamingContext.awaitTermination();

    }

    public static Connection createConnecttion() throws Exception {
        Class.forName("com.mysql.jdbc.Driver");
        return DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "root");
    }

}
