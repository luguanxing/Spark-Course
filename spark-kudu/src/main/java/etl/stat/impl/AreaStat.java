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

public class AreaStat implements SparkProcess {

    @Override
    public void process(JavaSparkContext jsc, SparkSession sparkSession) {
        // 从kudu读取数据
        Dataset kuduDF = KuduUtils.getOdsDF(sparkSession);
        kuduDF.show();

        // 执行sql语句
        kuduDF.createOrReplaceTempView("ods");
        Dataset<Row> areaTmpDF = sparkSession.sql(SqlUtils.AREA_SQL_STEP1);

        // 统计基本指标
        areaTmpDF.createOrReplaceTempView("area_tmp");
        areaTmpDF.show();

        // 使用基本指标统计数据
        Dataset<Row> statDF = sparkSession.sql(SqlUtils.AREA_SQL_STEP2);
        statDF.show();

        // 数据保存到kudu
        String tableName = "area_stat";
        String parititonKeys = "provincename";
        KuduUtils.saveDataToKudu(statDF, tableName, SchemaUtils.getAreaSchema(), parititonKeys);
    }

    public static void main(String[] args) {
        // 创建相关的context
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("stat-area");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        SparkSession sparkSession = new SparkSession(jsc.sc());

        // 进行统计
        new AreaStat().process(jsc, sparkSession);

        // 关闭资源
        jsc.stop();
    }

}
