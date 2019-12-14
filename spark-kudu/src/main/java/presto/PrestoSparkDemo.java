package presto;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class PrestoSparkDemo {

    public static void main(String[] args) {
        // 创建相关的context
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("PrestoSparkDemo");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        SparkSession sparkSession = new SparkSession(jsc.sc());

        // 读取presto数据
        Dataset<Row> prestoDF = sparkSession.read()
                .format("jdbc")
                .option("driver", "com.facebook.presto.jdbc.PrestoDriver")
                .option("url", "jdbc:presto://127.0.0.1:9999")
                //.option("dbtable", "mysql.test.tbl_wordcount")
                //.option("dbtable", "redis.presto_redis_schema.presto_redis_table")
                .option("dbtable", "kudu.default.ods")
                .option("user", "presto")
                .load();
        prestoDF.show();

        // 关闭资源
        jsc.stop();
    }

}
