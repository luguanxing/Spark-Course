package demo;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class SQLConetextDemo {

    // 运行该程序spark-core_2.11和spark-sql_2.11需要版本2.1.0
    public static void main(String[] args) {

        // 文件路径
        String path = "/Users/luguanxing/app/spark-2.2.0-bin-2.6.0-cdh5.7.0/examples/src/main/resources/people.json";

        // 创建相关的context
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("SQLConetextDemo");
        SparkContext sc = new SparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sc);

        // 相关处理
        Dataset<Row> people = sqlContext.read().format("json").load(path);
        people.printSchema();
        people.show();

        // 关闭资源
        sc.stop();

    }

}
