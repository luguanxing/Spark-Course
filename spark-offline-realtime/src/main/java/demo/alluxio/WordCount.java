package demo.alluxio;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {

    public static void main(String[] args) {
        // 创建相关的context
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("alluxio-wordcount");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        // 相关处理
        // String path = "src/main/resources/alluxio-wordcount";
        String path = "alluxio://localhost:19998/alluxio-spark/alluxio-wordcount";
        JavaRDD<String> lines = jsc.textFile(path, 1);
        lines.flatMap(
                line -> Arrays.asList(line.split(" ")).iterator()
        ).mapToPair(
                word -> new Tuple2<>(word, 1)
        ).reduceByKey(
                (cnt1, cnt2) -> cnt1 + cnt2
        ).foreach(
                tuple2 -> {
                    String word = tuple2._1;
                    Integer cnt = tuple2._2;
                    System.out.println(word + " -> " + cnt);
                }
        );

        // 关闭资源
        jsc.stop();

    }

}
