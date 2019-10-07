package dataframe;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class APIDemo {

    public static void main(String[] args) {

        // 文件路径
        String path = "/Users/luguanxing/app/spark-2.2.0-bin-2.6.0-cdh5.7.0/examples/src/main/resources/people.json";

        // 创建相关的context
        SparkSession sparkSession = SparkSession.builder()
                .appName("APIDemo")
                .master("local[2]")
                .getOrCreate();

        // 相关处理
        Dataset<Row> t_people = sparkSession.read().format("json").load(path);

        // 基本操作
        t_people.printSchema();
        t_people.show();
        t_people.select("name").show();

        // 对列做运算、别名
        t_people.select(t_people.col("name"), t_people.col("age").plus(10).as("ageAdd10")).show();

        // 过滤
        t_people.filter(t_people.col("age").$greater(20)).show();

        // 根据列分组统计数量
        t_people.groupBy("age").count().show();

        // 关闭资源
        sparkSession.stop();

    }

}
