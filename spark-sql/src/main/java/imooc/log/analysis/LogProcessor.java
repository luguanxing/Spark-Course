package imooc.log.analysis;

import imooc.log.analysis.utils.StructUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

// 从日志行读取数据，处理解析成df后保存到文件系统中
public class LogProcessor {

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession.builder()
                .appName("LogProcessor")
                .master("local[2]")
                .getOrCreate();

        // 读取日志，解析数据RDD
        String inputPath = "/Users/luguanxing/data/access.20161111.all/output";
        JavaRDD<String> accessDataRDD = sparkSession.sparkContext().textFile(inputPath, 1).toJavaRDD();
        JavaRDD<Row> accessRDD= accessDataRDD.map(
                line -> StructUtil.parseAccessInfo(line)
        );

        // 将RDD转换成DataFrame
        Dataset<Row> accessDF = sparkSession.createDataFrame(accessRDD, StructUtil.getStructType());
        accessDF.printSchema();
        accessDF.show(false);

        // 保存到外部格式
        String outputPath = "/Users/luguanxing/data/access.20161111.all/output_row";
        accessDF.write().parquet(outputPath);

        // 指定分区数、覆盖写模式
        // accessDF.coalesce(5).write().mode(SaveMode.Overwrite).partitionBy("day").parquet(outputPath);

        sparkSession.stop();

    }

}
