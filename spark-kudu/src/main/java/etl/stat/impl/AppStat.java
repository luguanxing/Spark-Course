package etl.stat.impl;

import etl.stat.SparkProcess;
import etl.utils.KuduUtils;
import etl.utils.SchemaUtils;
import etl.utils.SqlUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class AppStat implements SparkProcess {

    @Override
    public void process(JavaSparkContext jsc, SparkSession sparkSession) {
        // 从kudu读取数据
        Dataset kuduDF = KuduUtils.getOdsDF(sparkSession);
        kuduDF.show();

        // 执行sql语句
        kuduDF.createOrReplaceTempView("ods");
        Dataset<Row> areaTmpDF = sparkSession.sql(SqlUtils.APP_SQL_STEP1);

        // 统计基本指标
        areaTmpDF.createOrReplaceTempView("app_tmp");
        areaTmpDF.show();

        // 使用基本指标统计数据
        Dataset<Row> statDF = sparkSession.sql(SqlUtils.APP_SQL_STEP2);
        statDF.show();

        // 数据保存到kudu
        String tableName = "app_stat";
        String parititonKeys = "appid";
        KuduUtils.saveDataToKudu(statDF, tableName, SchemaUtils.getAppSchema(), parititonKeys);
    }

    public static void main(String[] args) {
        // 创建相关的context
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("stat-app");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        SparkSession sparkSession = new SparkSession(jsc.sc());

        // 进行统计
        new AppStat().process(jsc, sparkSession);

        // 关闭资源
        jsc.stop();
    }

}
