package project.offline;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import project.utils.HBaseUtil;
import project.utils.MysqlUtil;

import java.util.Arrays;
import java.util.List;

public class LogStat {

    public static void main(String[] args) throws Exception {
        // 创建相关的context
        SparkConf sparkConf = new SparkConf().setAppName("spark_read_hbase").setMaster("local[*]");
        SparkContext sc = new SparkContext(sparkConf);
        JavaSparkContext jsc = new JavaSparkContext(sc);
        SQLContext sqlContext = new SQLContext(sc);

        // 设置hbase数据查询条件
        String day = "20190130";
        String hbaseTableName = "access_" + day;
        Scan scan = new Scan();
        scan.addColumn("info".getBytes(), "ip".getBytes());
        scan.addColumn("info".getBytes(), "ipMainInfo".getBytes());
        scan.addColumn("info".getBytes(), "ipSubInfo".getBytes());
        scan.addColumn("info".getBytes(), "browserName".getBytes());
        scan.addColumn("info".getBytes(), "browserVersion".getBytes());
        scan.addColumn("info".getBytes(), "osName".getBytes());
        scan.addColumn("info".getBytes(), "osVersion".getBytes());

        // 将scan编码
        String scanToString = Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray());
        Configuration configuration = HBaseUtil.getConfiguration();
        configuration.set(TableInputFormat.INPUT_TABLE, hbaseTableName);
        configuration.set(TableInputFormat.SCAN, scanToString);

        // 将HBase数据转成RDD
        JavaPairRDD<ImmutableBytesWritable, Result> hbaseRdd = jsc.newAPIHadoopRDD(configuration, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
        JavaRDD<Row> rowRdd = hbaseRdd.map(
                tuple2 -> {
                    Result result = tuple2._2;
                    String rowKey = Bytes.toString(result.getRow());
                    String ip = Bytes.toString(result.getValue("info".getBytes(), "ip".getBytes()));
                    String ipMainInfo = Bytes.toString(result.getValue("info".getBytes(), "ipMainInfo".getBytes()));
                    String ipSubInfo = Bytes.toString(result.getValue("info".getBytes(), "ipSubInfo".getBytes()));
                    String browserName = Bytes.toString(result.getValue("info".getBytes(), "browserName".getBytes()));
                    String browserVersion = Bytes.toString(result.getValue("info".getBytes(), "browserVersion".getBytes()));
                    String osName = Bytes.toString(result.getValue("info".getBytes(), "osName".getBytes()));
                    String osVersion = Bytes.toString(result.getValue("info".getBytes(), "osVersion".getBytes()));
                    return RowFactory.create(rowKey, ip, ipMainInfo, ipSubInfo, browserName, browserVersion, osName, osVersion);
                }
        );

        // 构建DataFrame的schema，顺序必须与构建RowRDD的顺序一致，并将结果转化为DataFrame
        List<StructField> structFields = Arrays.asList(
                DataTypes.createStructField("rowKey", DataTypes.StringType, true),
                DataTypes.createStructField("ip", DataTypes.StringType, true),
                DataTypes.createStructField("ipMainInfo", DataTypes.StringType, true),
                DataTypes.createStructField("ipSubInfo", DataTypes.StringType, true),
                DataTypes.createStructField("browserName", DataTypes.StringType, true),
                DataTypes.createStructField("browserVersion", DataTypes.StringType, true),
                DataTypes.createStructField("osName", DataTypes.StringType, true),
                DataTypes.createStructField("osVersion", DataTypes.StringType, true)
        );
        StructType schema = DataTypes.createStructType(structFields);
        Dataset<Row> rowDF = sqlContext.createDataFrame(rowRdd, schema);

        // 创建临时视图表并进行sql统计，对ip去重
        rowDF.createOrReplaceTempView("t_data");
        sqlContext.sql("SELECT ip, count(1) as cnt FROM t_data GROUP BY ip ORDER BY cnt DESC").show();
        sqlContext.sql("SELECT ipMainInfo, count(1) as cnt FROM t_data GROUP BY ipMainInfo ORDER BY cnt DESC").show();
        sqlContext.sql("SELECT ipSubInfo, count(1) as cnt FROM t_data GROUP BY ipSubInfo ORDER BY cnt DESC").show();
        sqlContext.sql("SELECT browserName, count(1) as cnt FROM t_data GROUP BY browserName ORDER BY cnt DESC").show();
        sqlContext.sql("SELECT browserVersion, count(1) as cnt FROM t_data GROUP BY browserVersion ORDER BY cnt DESC").show();
        sqlContext.sql("SELECT osName, count(1) as cnt FROM t_data GROUP BY osName ORDER BY cnt DESC").show();
        sqlContext.sql("SELECT osVersion, count(1) as cnt FROM t_data GROUP BY osVersion ORDER BY cnt DESC").show();

        // 保存到mysql
        sqlContext.sql("SELECT ip, count(1) as cnt FROM t_data GROUP BY ip ORDER BY cnt DESC")
                .write()
                .mode(SaveMode.Overwrite)
                .jdbc("jdbc:mysql://127.0.0.1:3306/test", "ip_cnt", MysqlUtil.getProperties());

        // 关闭资源
        sc.stop();
    }

}
