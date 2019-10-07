package dataframe;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class RDDDemo_Reflection {

    public static void main(String[] args) {

        // 文件路径
        String path = "/Users/luguanxing/app/spark-2.2.0-bin-2.6.0-cdh5.7.0/examples/src/main/resources/people.txt";

        // 创建相关的context
        SparkSession sparkSession = SparkSession.builder()
                .appName("RDDDemo_Reflection")
                .master("local[2]")
                .getOrCreate();

        // 获取RDD
        JavaRDD<String> javaRDD = sparkSession.sparkContext().textFile(path, 1).toJavaRDD();
        JavaRDD<Person> personJavaRDD = javaRDD.map(
                line -> {
                    String[] infos = line.split(", ");
                    String name = infos[0];
                    Integer age = Integer.parseInt(infos[1]);
                    return new Person(name, age);
                }
        );

        // 创建DataFrame并查询
        Dataset<Row> personDataFrame = sparkSession.createDataFrame(personJavaRDD, Person.class);
        personDataFrame.show();
        personDataFrame.filter(personDataFrame.col("age").$greater(20)).show();

        // 创建临时视图表并写sql查询
        personDataFrame.createOrReplaceTempView("t_person");
        sparkSession.sql("SELECT * FROM t_person ORDER BY age DESC").show();

        // 关闭资源
        sparkSession.stop();

    }

}
