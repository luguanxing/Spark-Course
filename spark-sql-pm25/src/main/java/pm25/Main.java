package pm25;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.types.DataTypes;

public class Main {

    public static void main(String[] args) {
        // 指定数据源文件路径(ide环境运行用本地路径，hadoop环境运行用hdfs路径)
        String inputPath = "./src/main/resources/Beijing_*.csv";
        // String inputPath = "/pm25_data/Beijing_*.csv";

        // 创建相关的context
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("PM2.5-Demo");
        SparkContext sc = new SparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sc);

        // 读入数据源，清洗脏数据
        Dataset<Row> data = sqlContext.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(inputPath);
        data = data.filter(data.col("Value").$greater$eq(0));
        data.printSchema();
        data.show();

        // 添加评级列，根据年份和评级分组进行数量聚合统计
        sqlContext.udf().register("getGrade", getGrade, DataTypes.StringType);
        Dataset<Row> gradedCountYear = data.select("Year", "Month", "Day", "Hour", "Value")
                .withColumn("Grade", functions.callUDF("getGrade", data.col("Value")))
                .groupBy("Year", "Grade")
                .count()
                .orderBy("Year", "Grade", "count");
        gradedCountYear.show();

        // 添加时间列，统计数值的变化关系
        sqlContext.udf().register("getTime", getTime, DataTypes.StringType);
        Dataset<Row> timeValue = data.select("Year", "Month", "Day", "Hour", "Value")
                .withColumn("Time", functions.callUDF("getTime", data.col("Year"), data.col("Month"), data.col("Day"), data.col("Hour")))
                .select("Time", "Value")
                .orderBy("Time");
        timeValue.show(100);

        // 将数据写到ES中
        data.write().format("org.elasticsearch.spark.sql").option("es.node", "127.0.0.1:9200").mode(SaveMode.Overwrite).save("pm25_data/data");
        gradedCountYear.write().format("org.elasticsearch.spark.sql").option("es.node", "127.0.0.1:9200").mode(SaveMode.Overwrite).save("pm25_graded_count_year/data");
        timeValue.write().format("org.elasticsearch.spark.sql").option("es.nodes", "127.0.0.1:9200").mode(SaveMode.Overwrite).save("pm25_time_value/data");

        // 关闭资源
        sc.stop();
    }

    // 自定义的评级函数，用于给PM2.5评级
    private static UDF1 getGrade = new UDF1<Integer, String>() {
        public String call(final Integer value) throws Exception {
            if (0 <= value && value < 50) {
                return "健康";
            } else if (50 <= value && value < 100) {
                return "中等";
            } else if (100 <= value && value < 150) {
                return "敏感人群不健康";
            } else if (150 <= value && value < 200) {
                return "中等";
            } else if (200 <= value && value < 250) {
                return "不健康";
            } else if (250 <= value && value < 300) {
                return "非常不健康";
            } else if (300 <= value && value < 500) {
                return "危险";
            } else {
                return "爆表";
            }
        }
    };

    // 添加时间字符串函数，用于统计值随时间变化
    private static UDF4 getTime = new UDF4<Integer, Integer, Integer, Integer, String>() {
        public String call(Integer year, final Integer month, final Integer day, final Integer hour) throws Exception {
            return year + "-" + (month>=10?month:"0"+month) + "-" + (day>=10?day:"0"+day) + " " + (hour>=10?hour:"0"+hour) + ":00:00";
        }
    };

}
