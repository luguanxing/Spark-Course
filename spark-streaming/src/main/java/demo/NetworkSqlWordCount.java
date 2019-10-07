package demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Arrays;
import java.util.Iterator;

public class NetworkSqlWordCount {

    public static class JavaRecord implements java.io.Serializable {
        private String word;

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }
    }


    public static void main(String[] args) throws Exception {

        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("demo.NetworkWordCount");

        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        JavaReceiverInputDStream<String> lines = streamingContext.socketTextStream("127.0.0.1", 12345);

        JavaDStream<String> wordDStream = lines.flatMap(
                line -> {
                    String[] words = line.split(" ");
                    Iterator<String> iterator = Arrays.asList(words).iterator();
                    return iterator;
                }
        );

        wordDStream.foreachRDD(
                rdd -> {
                    // 创建sparkSession
                    SparkSession sparkSession = SparkSession.builder().config(rdd.context().getConf()).getOrCreate();

                    // 转换成包装类
                    JavaRDD<JavaRecord> rowRDD = rdd.map(
                            word -> {
                                JavaRecord record = new JavaRecord();
                                record.setWord(word);
                                return record;
                            }
                    );

                    // 创建DataFrame
                    Dataset<Row> wordsDataFrame = sparkSession.createDataFrame(rowRDD, JavaRecord.class);

                    // 创建视图
                    wordsDataFrame.createOrReplaceTempView("tbl_word");

                    // 使用sql语句统计数据
                    Dataset<Row> wordCountsDataFrame = sparkSession.sql("SELECT word, COUNT(*) AS cnt FROM tbl_word GROUP BY word ORDER BY cnt DESC");

                    // 展示数据
                    wordCountsDataFrame.show();
                }
        );

        streamingContext.start();
        streamingContext.awaitTermination();

    }

}
