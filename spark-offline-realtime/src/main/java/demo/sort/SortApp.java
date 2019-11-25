package demo.sort;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class SortApp {

    public static void main(String[] args) {
        // 创建相关的context
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("sortApp");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        // 自定义排序，先按价格降序，再按库存升序
        List<String> list = Arrays.asList("充电宝 200 10,手机 1500 1000,电视 2000 1000,手表 200 300,电脑 2000 50,平板 1200 30".split(","));
        JavaRDD<String> itemStringRdd = jsc.parallelize(list);
        itemStringRdd.map(
                itemString -> {
                    String[] infos = itemString.split(" ");
                    String itemName = infos[0];
                    Integer itemPrice = Integer.parseInt(infos[1]);
                    Integer itemCount = Integer.parseInt(infos[2]);
                    return new Item(itemName, itemPrice, itemCount);
                }
        ).sortBy(
                item -> item, false, 1
        ).foreach(
                item -> System.out.println(item)
        );

        // 关闭资源
        jsc.stop();
    }

    private static class Item implements Comparable<Item>, Serializable {
        private String itemName;
        private Integer itemPrice;
        private Integer itemCount;
        public Item(String itemName, Integer price, Integer count) {
            this.itemName = itemName;
            this.itemPrice = price;
            this.itemCount = count;
        }
        @Override
        public int compareTo(Item o) {
            // 两个Integer对象不支持使用==自动拆箱比较
            if (itemPrice.intValue() == o.itemPrice) {
                return o.itemCount - itemCount;
            }
            return itemPrice - o.itemPrice;
        }
        @Override
        public String toString() {
            return "Item{" + itemName + ',' + itemPrice + "," + itemCount + '}';
        }
    }

}
