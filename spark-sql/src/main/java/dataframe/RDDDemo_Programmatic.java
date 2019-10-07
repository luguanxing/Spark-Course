package dataframe;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class RDDDemo_Programmatic {

    public static void main(String[] args) {

        // 文件路径
        String path = "/Users/luguanxing/app/spark-2.2.0-bin-2.6.0-cdh5.7.0/examples/src/main/resources/people.txt";

        // 创建相关的context
        SparkSession sparkSession = SparkSession.builder()
                .appName("RDDDemo_Programmatic")
                .master("local[2]")
                .getOrCreate();

        // 获取RDD
        JavaRDD<String> javaRDD = sparkSession.sparkContext().textFile(path, 1).toJavaRDD();
        JavaRDD<Row> rowJavaRDD = javaRDD.map(
                line -> {
                    String[] infos = line.split(", ");
                    String name = infos[0];
                    Integer age = Integer.parseInt(infos[1]);
                    Row row = RowFactory.create(name, age);
                    return row;
                }
        );

        // 创建schema
        List<StructField> fields = new ArrayList<>();
        StructField field_name = DataTypes.createStructField("name", DataTypes.StringType, true);
        StructField field_age = DataTypes.createStructField("age", DataTypes.IntegerType, true);
        fields.add(field_name);
        fields.add(field_age);
        StructType schema = DataTypes.createStructType(fields);

        // 把RDD和schema关联起来并查询
        Dataset<Row> personDataFrame = sparkSession.createDataFrame(rowJavaRDD, schema);
        personDataFrame.printSchema();
        personDataFrame.show();
        personDataFrame.filter(personDataFrame.col("age").$greater(20)).show();

        // 创建临时视图表并写sql查询
        personDataFrame.createOrReplaceTempView("t_person");
        sparkSession.sql("SELECT * FROM t_person ORDER BY age DESC").show();

        // 关闭资源
        sparkSession.stop();

    }

}
