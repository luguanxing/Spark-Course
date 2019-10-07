package dataframe;

import com.google.gson.Gson;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class RDDTest {

    public static void main(String[] args) {

        // 文件路径
        String path = "/Users/luguanxing/app/spark-2.2.0-bin-2.6.0-cdh5.7.0/examples/src/main/resources/people.json";

        // 创建相关的context
        SparkSession sparkSession = SparkSession.builder()
                .appName("RDDTest")
                .master("local[2]")
                .getOrCreate();

        // 获取RDD
        JavaRDD<String> javaRDD = sparkSession.sparkContext().textFile(path, 1).toJavaRDD();
        JavaRDD<Person> personJavaRDD = javaRDD.map(
                line -> new Gson().fromJson(line, Person.class)
        );

        // 创建DataFrame并查询
        Dataset<Row> personDataFrame = sparkSession.createDataFrame(personJavaRDD, Person.class);
        personDataFrame.show(5);
        personDataFrame.limit(5).show();
        personDataFrame.filter(personDataFrame.col("age").isNotNull()).sort(personDataFrame.col("age").desc()).show();

        // 创建临时视图表并写sql查询
        personDataFrame.createOrReplaceTempView("t_person");
        sparkSession.sql("SELECT * FROM t_person WHERE name IS NOT NULL AND age IS NOT NULL ORDER BY age DESC").show();
        sparkSession.sql("SELECT * FROM t_person AS t1 join t_person AS t2 WHERE t1.name == t2.name").show();

        // 关闭资源
        sparkSession.stop();
    }

}
