package project.offline;

import project.domain.LogInfo;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import project.utils.HBaseUtil;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.zip.CRC32;

public class LogETL {

    public static void main(String[] args) {
        // 文件路径
        String path = "/Users/luguanxing/test.access.log";

        // 创建相关的context
        SparkConf sparkConf = new SparkConf().setAppName("spark_etl").setMaster("local[*]");
        SparkContext sc = new SparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sc);

        // 获取RDD
        JavaRDD<String> javaRDD = sc.textFile(path, 1).toJavaRDD();
        JavaRDD<LogInfo> logInfoRDD = javaRDD.map(log -> LogInfo.convertToLogInfo(log));

        // 创建DataFrame
        Dataset<Row> loginfoDataFrame = sqlContext.createDataFrame(logInfoRDD, LogInfo.class);

        // 格式化dataFrame并添加时间列
        sqlContext.udf().register("formatTime", formatTime, DataTypes.StringType);
        Dataset<Row> formattedDataFrame = loginfoDataFrame
                .withColumn("formatTime", functions.callUDF("formatTime", loginfoDataFrame.col("time")))
                .select("ip", "ipMainInfo", "ipSubInfo", "formatTime", "method", "url", "protocal", "status", "bytesent", "referer", "browserName", "browserVersion", "osName", "osVersion", "userAgent");

        // 数据落地到hbase中
        String day = "20190130";
        String hbaseTableName = "accessV2_" + day;
        HBaseUtil.createTable(hbaseTableName);
        String[] rowColumns = {"ip", "ipMainInfo", "ipSubInfo", "formatTime", "method", "url", "protocal", "status", "bytesent", "referer", "browserName", "browserVersion", "osName", "osVersion", "userAgent"};
        formattedDataFrame.foreachPartition(
                partitonOfRecords -> {
                    List<Put> puts = new ArrayList<>();
                    while (partitonOfRecords.hasNext()) {
                        Row row = partitonOfRecords.next();
                        String rowKey = getRowKey(row);
                        Put put = new Put(rowKey.getBytes());
                        for (String rowColumn : rowColumns) {
                            String rowColumnValue = row.getAs(rowColumn).toString();
                            put.addColumn("info".getBytes(), rowColumn.getBytes(), rowColumnValue.getBytes());
                        }
                        put.setDurability(Durability.SKIP_WAL); // 禁用WAL
                        puts.add(put);
                    }
                    HBaseUtil.writeBatch(hbaseTableName, puts);
                }
        );

        // 刷新表格，使hbase内存数据落地
        HBaseUtil.refreshTable(hbaseTableName);

        // 关闭资源
        sc.stop();
    }

    private static String getRowKey(Row row) {
        // 设计rowkey为时间加上全部信息的md5
        String rowFormatTime = row.getAs("formatTime").toString();
        CRC32 crc32 = new CRC32();
        crc32.reset();
        crc32.update(row.toString().getBytes());
        long rowCRC = crc32.getValue();
        return rowFormatTime + "_" + rowCRC;
    }

    // 自定义的格式化函数
    private static UDF1 formatTime = (UDF1<String, String>) timeString -> {
        String time = timeString.substring(timeString.indexOf("[") + 1, timeString.lastIndexOf("]"));
        Date dateTime = FastDateFormat.getInstance("dd/MMM/yyy:HH:mm:ss Z", Locale.ENGLISH).parse(time);
        String formatTime = FastDateFormat.getInstance("yyyyMMddHHmmss").format(dateTime);
        return formatTime;
    };


}