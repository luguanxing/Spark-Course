package dataframe;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataSetDemo {

    public static void main(String[] args) {

        // 文件路径
        String path = "/Users/luguanxing/app/spark-2.2.0-bin-2.6.0-cdh5.7.0/examples/src/main/resources/people.csv";

        // 创建相关的context
        SparkSession sparkSession = SparkSession.builder()
                .appName("DataSetDemo")
                .master("local[2]")
                .getOrCreate();

        // 获取dataframe
        Dataset<Row> dataFrame = sparkSession.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(path);
        dataFrame.printSchema();
        dataFrame.show();

        // 转换成dataset
        Dataset<Person> dataSet = dataFrame.as(Encoders.bean(Person.class));
        dataSet.map(
                (Person p) -> p.name, Encoders.STRING()
        ).show();

        // 关闭资源
        sparkSession.stop();
    }

}
