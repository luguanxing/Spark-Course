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

public class ProvinceCityStat implements SparkProcess {

    @Override
    public void process(JavaSparkContext jsc, SparkSession sparkSession) {
        // 从kudu读取数据
        Dataset kuduDF = KuduUtils.getOdsDF(sparkSession);
        kuduDF.printSchema();
        kuduDF.show();

        // 执行sql语句
        kuduDF.createOrReplaceTempView("ods");
        Dataset<Row> statDF = sparkSession.sql(SqlUtils.PROVINCE_CITY_SQL);

        // 统计分组topn
        statDF.createOrReplaceTempView("stat");
        sparkSession.sql("SELECT provincename, cityname, cnt, RANK() OVER (PARTITION BY provincename ORDER BY cnt DESC) AS ranks FROM stat HAVING ranks <= 3 ORDER BY provincename, cnt DESC").show();

        // 数据保存到kudu
        String tableName = "province_city_cnt";
        String parititonKeys = "provincename";
        KuduUtils.saveDataToKudu(statDF, tableName, SchemaUtils.getProvinceCitySchema(), parititonKeys);
    }

    public static void main(String[] args) {
        // 创建相关的context
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("stat-province-city");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        SparkSession sparkSession = new SparkSession(jsc.sc());

        // 进行统计
        new ProvinceCityStat().process(jsc, sparkSession);

        // 关闭资源
        jsc.stop();
    }

}
