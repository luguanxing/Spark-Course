package demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class JoinDemo {

    public static void main(String[] args) {
        // 创建相关的context
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("JoinDemo");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        // 使用join方法关联数据
        List<IdName> idNames = new ArrayList<>();
        idNames.add(new IdName("100", "test"));
        idNames.add(new IdName("101", "van"));
        idNames.add(new IdName("102", "zhangsan"));

        List<IdInfo> idInfos = new ArrayList<>();
        idInfos.add(new IdInfo("100", "ustc", "shanghai"));
        idInfos.add(new IdInfo("101", "pku", "beijing"));
        idInfos.add(new IdInfo("103", "dark", "tokyo"));

        JavaRDD<IdName> idNameJavaRDD = jsc.parallelize(idNames);
        JavaRDD<IdInfo> idInfoJavaRDD = jsc.parallelize(idInfos);

        JavaPairRDD<String, IdName> idNamePairRDD = idNameJavaRDD.mapToPair(idName -> new Tuple2<>(idName.id, idName));
        JavaPairRDD<String, IdInfo> idInfoPairRDD = idInfoJavaRDD.mapToPair(idInfo -> new Tuple2<>(idInfo.id, idInfo));
        idNamePairRDD.join(idInfoPairRDD)
                .map(
                        tuple2 -> {
                            String id = tuple2._1;
                            String name = tuple2._2._1.name;
                            String school = tuple2._2._2.school;
                            String city = tuple2._2._2.city;
                            return new IdNameInfo(id, name, school, city);
                        }
                )
                .foreach(
                        idNameInfo -> System.err.println(idNameInfo)
                );

        // 关闭资源
        jsc.stop();
    }

    public static class IdName implements Serializable {
        String id;
        String name;

        public IdName(String id, String name) {
            this.id = id;
            this.name = name;
        }
    }

    public static class IdInfo implements Serializable {
        String id;
        String school;
        String city;

        public IdInfo(String id, String school, String city) {
            this.id = id;
            this.school = school;
            this.city = city;
        }
    }

    public static class IdNameInfo implements Serializable {
        String id;
        String name;
        String school;
        String city;

        public IdNameInfo(String id, String name, String school, String city) {
            this.id = id;
            this.name = name;
            this.school = school;
            this.city = city;
        }

        @Override
        public String toString() {
            return "IdNameInfo{" +
                    "id='" + id + '\'' +
                    ", name='" + name + '\'' +
                    ", school='" + school + '\'' +
                    ", city='" + city + '\'' +
                    '}';
        }
    }

}
