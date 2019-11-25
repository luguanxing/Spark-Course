package demo.broadcast;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Serializable;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class BroadCastVariable {

    public static void main(String[] args) {
        // 创建相关的context
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("test-variable");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        // 相关处理
        String path = "src/main/resources/alluxio-wordcount";
        JavaRDD<String> lines = jsc.textFile(path, 1);

        // 定义广播变量
        MyBroadCastVariable outVar = new MyBroadCastVariable();
        Broadcast<MyBroadCastVariable> broadcast = jsc.broadcast(outVar);

        lines.flatMap(
                line -> Arrays.asList(line.split(" ")).iterator()
        ).mapToPair(
                word -> {
                    // 使用外部变量，如果不是广播变量需要下发到每个task中，是广播变量只需要下发到executor
                    MyBroadCastVariable inVar = broadcast.getValue();
                    System.out.println(inVar);
                    return new Tuple2<>(word, 1);
                }
        ).reduceByKey(
                (cnt1, cnt2) -> cnt1 + cnt2
        ).foreach(
                tuple2 -> System.out.println(tuple2)
        );

        // 关闭资源
        jsc.stop();
    }

    private static class MyBroadCastVariable implements Serializable {
        String A;
        String B;
        String C;
        List<String> list;
        Map<String, String> map;
    }

}
