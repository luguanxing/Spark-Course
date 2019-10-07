package datasource;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ParquetDemo {

    public static void main(String[] args) {

        // 文件路径
        String sourcePath = "/Users/luguanxing/app/spark-2.2.0-bin-2.6.0-cdh5.7.0/examples/src/main/resources/users.parquet";
        String destPath = "/Users/luguanxing/app/spark-2.2.0-bin-2.6.0-cdh5.7.0/examples/src/main/resources/users-filter.parquet";

        // 创建相关的context
        SparkSession sparkSession = SparkSession.builder()
                .appName("ParquetDemo")
                .master("local[2]")
                .getOrCreate();

        // 相关处理
        Dataset<Row> users = sparkSession.read().parquet(sourcePath);
        users.printSchema();
        users.show();
        users.select("name", "favorite_color").show();

        // 导出数据
        users.select("name", "favorite_color").write().json(destPath);

        // 关闭资源
        sparkSession.stop();

    }

}
