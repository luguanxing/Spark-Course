package datasource;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MysqlDemo {

    public static void main(String[] args) {

        // 创建相关的context
        SparkSession sparkSession = SparkSession.builder()
                .appName("MysqlDemo")
                .master("local[2]")
                .getOrCreate();

        // 操作mysql的数据
        Dataset<Row> mysqlDF = sparkSession.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/sparksql")
                .option("dbtable", "sparksql.TBLS")
                .option("user", "root")
                .option("password", "root")
                .load();
        mysqlDF.select("TBL_ID", "TBL_NAME").show();

        // 关闭资源
        sparkSession.stop();

    }

}
