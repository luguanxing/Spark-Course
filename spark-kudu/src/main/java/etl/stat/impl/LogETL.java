package etl.stat.impl;

import etl.common.IpData;
import etl.stat.SparkProcess;
import etl.utils.IpUtils;
import etl.utils.KuduUtils;
import etl.utils.SchemaUtils;
import etl.utils.SqlUtils;
import org.apache.kudu.Schema;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

public class LogETL implements SparkProcess {

    @Override
    public void process(JavaSparkContext jsc, SparkSession sparkSession) {
        // 加载日志数据
        String dataPath = "src/main/resources/data-test.json";
        Dataset<Row> jsonDF = sparkSession.read().json(dataPath);

        // 获取ip范围数据
        String ipPath = "src/main/resources/ip.txt";
        JavaRDD<IpData> ipDataRDD = jsc.textFile(ipPath)
                .map(
                        ipData -> {
                            String[] ipInfos = ipData.split("\\|");
                            long start = Long.parseLong(ipInfos[2]);
                            long end = Long.parseLong(ipInfos[3]);
                            String province = ipInfos[6];
                            String city = ipInfos[7];
                            String isp = ipInfos[9];
                            return new IpData(start, end, province, city, isp);
                        }
                );
        Dataset<Row> ipDataDF = sparkSession.createDataFrame(ipDataRDD, IpData.class);

        // 解析日志数据中的ip数据，需要先将ip转换成对应数值
        sparkSession.udf().register("getIpLong", getIpLong, DataTypes.LongType);
        jsonDF = jsonDF.withColumn("ip_long", functions.callUDF("getIpLong", jsonDF.col("ip")));

        // sql方式关联，关联条件是日志ip的数值在对应的ip范围数据中
        jsonDF.createOrReplaceTempView("data");
        ipDataDF.createOrReplaceTempView("ip_info");
        Dataset<Row> resultDF = sparkSession.sql(SqlUtils.SQL);

        // 数据落地kudu
        String tableName = "ods";
        Schema schema = SchemaUtils.getOdsSchema();
        String paritionKeys = "ip";
        KuduUtils.saveDataToKudu(resultDF, tableName, schema, paritionKeys);
    }

    public static void main(String[] args) {
        // 创建相关的context
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("log-etl");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        SparkSession sparkSession = new SparkSession(jsc.sc());

        // ETL处理
        new LogETL().process(jsc, sparkSession);

        // 关闭资源
        jsc.stop();
    }

    // 自定义ip转long函数
    private static UDF1 getIpLong = (UDF1<String, Long>) ip -> IpUtils.getIpLong(ip);

}
