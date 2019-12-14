package demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JoinDemoV2 {

    public static void main(String[] args) {
        // 创建相关的context
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("JoinDemoV2");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        // 小表转化成map变量进行广播
        Map<String, IdName> idNamesMap = new HashMap();
        idNamesMap.put("100", new IdName("100", "test"));
        idNamesMap.put("101", new IdName("101", "van"));
        idNamesMap.put("102", new IdName("102", "zhangsan"));

        List<IdInfo> idInfos = new ArrayList<>();
        idInfos.add(new IdInfo("100", "ustc", "shanghai"));
        idInfos.add(new IdInfo("101", "pku", "beijing"));
        idInfos.add(new IdInfo("103", "dark", "tokyo"));

        Broadcast<Map<String, IdName>> broadcast = jsc.broadcast(idNamesMap);
        JavaRDD<IdInfo> idInfoJavaRDD = jsc.parallelize(idInfos);

        // 遍历大表数据，遍历过程使用缓存的小表，相当于左连接
        idInfoJavaRDD.map(
                idInfo -> {
                    Map<String, IdName> bcIdNamesMap = broadcast.value();
                    String id = idInfo.id;
                    String school = idInfo.school;
                    String city = idInfo.city;
                    if (bcIdNamesMap.containsKey(id)) {
                        IdName idName = bcIdNamesMap.get(idInfo.id);
                        String name = idName.name;
                        return new IdNameInfo(id, name, school, city);
                    } else {
                        return new IdNameInfo(id, null, school, city);
                    }
                }
        ).foreach(
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
