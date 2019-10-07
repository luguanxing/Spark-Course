package log2flume2kafka2spark.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseUtil {

    private static HBaseUtil hBaseUtilInstance = null;

    private HBaseAdmin hBaseAdmin = null;

    private Configuration configuration = null;

    // 私有方法，只能通过单例模式获取
    private HBaseUtil() {
        configuration = new Configuration();
        configuration.set(HConstants.ZOOKEEPER_QUORUM, "127.0.0.1:2181");
        configuration.set(HConstants.HBASE_DIR, "hdfs://127.0.0.1:8020/hbase");
        try {
            hBaseAdmin = new HBaseAdmin(configuration);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 单例模式初始化
    public static synchronized HBaseUtil getHBaseUtilInstance() {
        if (hBaseUtilInstance == null) {
            hBaseUtilInstance = new HBaseUtil();
        }
        return hBaseUtilInstance;
    }

    public HTable getTable(String tableName) {
        HTable table = null;
        try {
            table = new HTable(configuration, tableName);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return table;
    }

    public void putData(String tableName, String rowkey, String cf, String column, String value) {
        HTable table = getTable(tableName);
        Put put = new Put(Bytes.toBytes(rowkey));
        put.add(Bytes.toBytes(cf), Bytes.toBytes(column), Bytes.toBytes(value));
        try {
            table.put(put);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        // 测试获取表
        // HTable table = HBaseUtil.getHBaseUtilInstance().getTable("course_clickcount");
        // System.out.println(table.getName().getNameAsString());

        // 测试写数据到表中
        String tableName = "course_clickcount";
        String rowkey = "20181111_88";
        String cf = "info";
        String column = "click_count";
        String value = "2";
        HBaseUtil.getHBaseUtilInstance().putData(tableName, rowkey, cf, column, value);
    }

}
