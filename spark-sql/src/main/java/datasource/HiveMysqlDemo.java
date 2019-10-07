package datasource;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HiveMysqlDemo {

    public static void main(String[] args) {

        // 创建相关的context
        SparkSession sparkSession = SparkSession.builder()
                .appName("HiveMysqlDemo")
                .enableHiveSupport()
                .master("local[2]")
                .getOrCreate();

        // hive的数据
        Dataset<Row> hive_table = sparkSession.sql("SELECT context FROM hive_wordcount");
        hive_table.show();
        hive_table.createOrReplaceTempView("hive_table");

        // mysql的数据
        Dataset<Row> mysqlDF = sparkSession.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/sparksql")
                .option("dbtable", "sparksql.TBLS")
                .option("user", "root")
                .option("password", "root")
                .load();
        Dataset<Row> mysql_table = mysqlDF.select("TBL_ID", "TBL_NAME");
        mysql_table.show();
        mysql_table.createOrReplaceTempView("mysql_table");

        // 两表进行关联
        sparkSession.sql("SELECT * FROM hive_table JOIN mysql_table ON mysql_table.TBL_NAME != hive_table.context").show();

        // 关闭资源
        sparkSession.stop();

    }

}
