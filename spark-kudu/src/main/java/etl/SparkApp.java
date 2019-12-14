package etl;

import etl.stat.impl.AppStat;
import etl.stat.impl.AreaStat;
import etl.stat.impl.LogETL;
import etl.stat.impl.ProvinceCityStat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkApp {

    public static void main(String[] args) {
        // 创建相关的context
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkApp");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        SparkSession sparkSession = new SparkSession(jsc.sc());

        // 数据ETL
        new LogETL().process(jsc, sparkSession);

        // 统计省份城市
        new ProvinceCityStat().process(jsc, sparkSession);

        // 统计区域指标
        new AreaStat().process(jsc, sparkSession);

        // 统计app指标
        new AppStat().process(jsc, sparkSession);

        // 关闭资源
        jsc.stop();
    }

}
