package demo;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

public class HiveConetextDemo {

    // 运行该程序spark-core_2.11和spark-sql_2.11需要版本2.1.0
    public static void main(String[] args) {

        // 创建相关的context
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("HiveConetextDemo");
        SparkContext sc = new SparkContext(sparkConf);
        HiveContext hiveContext = new HiveContext(sc);

        // 相关处理
        hiveContext.table("hive_wordcount").show();

        // 关闭资源
        sc.stop();

    }

}
