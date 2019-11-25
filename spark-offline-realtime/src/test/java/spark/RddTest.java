package spark;

import project.domain.LogInfo;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import java.util.Date;
import java.util.Locale;

public class RddTest {

    public static void main(String[] args) {
        // 文件路径
        String path = "/Users/luguanxing/test.access.log";

        // 创建相关的context
        SparkConf sparkConf = new SparkConf().setAppName("test spark rdd").setMaster("local[*]");
        SparkContext sc = new SparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sc);

        // 获取RDD
        JavaRDD<String> javaRDD = sc.textFile(path, 1).toJavaRDD();
        JavaRDD<LogInfo> logInfoRDD = javaRDD.map(log -> LogInfo.convertToLogInfo(log));

        // 创建DataFrame
        Dataset<Row> loginfoDataFrame = sqlContext.createDataFrame(logInfoRDD, LogInfo.class);
        loginfoDataFrame.show(false);

        // 格式化dataFrame并添加时间列
        sqlContext.udf().register("formatTime", formatTime, DataTypes.StringType);
        Dataset<Row> formattedDataFrame = loginfoDataFrame
                .withColumn("formatTime", functions.callUDF("formatTime", loginfoDataFrame.col("time")))
                .select("ip", "ipMainInfo", "ipSubInfo", "formatTime", "method", "url", "protocal", "status", "bytesent", "referer", "browserName", "browserVersion", "osName", "osVersion", "userAgent");
        formattedDataFrame.show();

        // 关闭资源
        sc.stop();
    }

    // 自定义的格式化函数
    private static UDF1 formatTime = (UDF1<String, String>) timeString -> {
        String time = timeString.substring(timeString.indexOf("[") + 1, timeString.lastIndexOf("]"));
        Date dateTime = FastDateFormat.getInstance("dd/MMM/yyy:HH:mm:ss Z", Locale.ENGLISH).parse(time);
        String formatTime = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").format(dateTime);
        return formatTime;
    };

}
