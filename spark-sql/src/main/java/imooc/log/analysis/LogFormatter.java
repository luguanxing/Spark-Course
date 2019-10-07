package imooc.log.analysis;

import imooc.log.analysis.utils.DateUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

// 从原始日志读取数据，提取出所需信息，保存成日志行
public class LogFormatter {

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession.builder()
                .appName("LogFormatter")
                .master("local[2]")
                .getOrCreate();

        // 读取日志
        String inputPath = "/Users/luguanxing/data/access.20161111.log";
        JavaRDD<String> accessLog = sparkSession.sparkContext().textFile(inputPath, 1).toJavaRDD();

        // 对数据处理
        String outputPath = "/Users/luguanxing/data/access.20161111.all/output";
        accessLog.map(
                line -> {
                    String[] infos = line.split(" ");
                    String ip = infos[0];
                    String timeStr = infos[3] + infos[4];
                    String time = DateUtil.parseTimeStr(timeStr);
                    String url = infos[11].replaceAll("\"", "");
                    String traffic = infos[9];
                    return time + "\t" + ip + "\t" + url + "\t" + traffic;
                }
        ).saveAsTextFile(outputPath);

        sparkSession.stop();

    }

}
