package demo;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Serializable;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SparkKudu {

    public static void main(String[] args) {
        // 创建相关的context
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("spark-kudu");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        SparkSession sparkSession = new SparkSession(jsc.sc());

        // 相关处理
        String path = "src/main/resources/wordcount";
        JavaRDD<String> lines = jsc.textFile(path, 1);

        // 读取文件rdd，计算wordcount
        JavaRDD<Row> rowRdd = lines.flatMap(
                line -> Arrays.asList(line.split(" ")).iterator()
        ).mapToPair(
                word -> new Tuple2<>(word, 1)
        ).reduceByKey(
                (cnt1, cnt2) -> cnt1 + cnt2
        ).map(
                tuple2 -> {
                    String word = tuple2._1;
                    Integer count = tuple2._2;
                    Row row = RowFactory.create(word, count);
                    return row;
                }
        );

        // 创建schema，注意对应kudu创建的表字段
        List<StructField> fields = new ArrayList<>();
        StructField field_key = DataTypes.createStructField("key", DataTypes.StringType, true);
        StructField field_value = DataTypes.createStructField("value", DataTypes.IntegerType, true);
        fields.add(field_key);
        fields.add(field_value);
        StructType schema = DataTypes.createStructType(fields);

        // 转换RDD并查询
        Dataset<Row> wordCountDF = sparkSession.createDataFrame(rowRdd, schema);
        wordCountDF.printSchema();
        wordCountDF.show();

        // 新建表
        createTable();

        // 写到kudu中
        wordCountDF.write()
                .mode(SaveMode.Append)
                .format("org.apache.kudu.spark.kudu")
                .option("kudu.master", "vmware-centos")
                .option("kudu.table", "wordcount")
                .save();

        // 读kudu数据
        Dataset<Row> kuduDF = sparkSession.read()
                .format("org.apache.kudu.spark.kudu")
                .option("kudu.master", "vmware-centos")
                .option("kudu.table", "wordcount")
                .load();
        kuduDF.printSchema();
        kuduDF.show();

        // 删除表
        deleteTable();

        // 关闭资源
        jsc.stop();
    }

    public static void createTable() {
        try {
            KuduClient client = new KuduClient.KuduClientBuilder("vmware-centos").build();
            List<ColumnSchema> columns = new ArrayList<>();
            columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.STRING).key(true).build());
            columns.add(new ColumnSchema.ColumnSchemaBuilder("value", Type.INT32).nullable(true).build());
            Schema schema = new Schema(columns);
            CreateTableOptions cto = new CreateTableOptions();
            List<String> hashKeys = Arrays.asList("key");
            cto.addHashPartitions(hashKeys, 3);
            cto.setNumReplicas(1);
            client.createTable("wordcount", schema, cto);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void deleteTable() {
        try {
            KuduClient client = new KuduClient.KuduClientBuilder("vmware-centos").build();
            client.deleteTable("wordcount");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
