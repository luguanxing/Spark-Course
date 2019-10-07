package datasource;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HiveDemo {

    public static void main(String[] args) {

        // 创建相关的context
        SparkSession sparkSession = SparkSession.builder()
                .appName("HiveDemo")
                .enableHiveSupport()
                .master("local[2]")
                .getOrCreate();

        // 相关处理
        sparkSession.sql("show tables").show();
        sparkSession.sql("SELECT COUNT(1) AS cnt FROM hive_wordcount").write().saveAsTable("hive_wordcount_cnt");

        // 关闭资源
        sparkSession.stop();

    }

}
