package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class InitTest {

    public static void main(String[] args) {
        // 初始化spark
        SparkConf sparkConf = new SparkConf().setAppName("test spark").setMaster("local[*]");
        SparkContext sc = new SparkContext(sparkConf);
        JavaSparkContext jsc = new JavaSparkContext(sc);

        // 测试spark运行
        List<String> list = Arrays.asList("A", "B", "C");
        JavaRDD<String> javaRDD = jsc.parallelize(list);
        javaRDD.collect().forEach(
                rdd -> {
                    System.out.println(rdd);
                }
        );

        // 关闭资源
        sc.stop();
    }

}
