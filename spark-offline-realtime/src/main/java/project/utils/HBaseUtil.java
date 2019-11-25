package project.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.util.List;

public class HBaseUtil {

    private static Configuration configuration = null;
    private static Connection connection = null;
    private static Admin admin = null;

    // 初始化hbase配置
    private static void init() {
        try {
            configuration = new Configuration();
            configuration.set("hbase.rootdir", "hdfs://127.0.0.1:8020/hbase");
            configuration.set("hbase.zookeeper.quorum", "127.0.0.1:2181");
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 创建hbase表格
    public static void createTable(String hbaseTableName) {
        if (configuration == null || connection == null || admin == null) {
            init();
        }
        try {
            TableName tableName = TableName.valueOf(hbaseTableName);
            if (admin.tableExists(tableName)) {
                // 当前表格存在时进行覆盖
                admin.disableTable(TableName.valueOf(hbaseTableName));
                admin.deleteTable(TableName.valueOf(hbaseTableName));
            }
            // 新建表格
            HTableDescriptor descriptor = new HTableDescriptor(tableName);
            descriptor.addFamily(new HColumnDescriptor("info"));
            admin.createTable(descriptor);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 批量写hbase数据
    public static void writeBatch(String hbaseTableName, List<Put> puts) {
        if (configuration == null || connection == null || admin == null) {
            init();
        }
        try {
            Table table = connection.getTable(TableName.valueOf(hbaseTableName));
            table.put(puts);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 获取配置文件
    public static Configuration getConfiguration() {
        if (configuration == null || connection == null || admin == null) {
            init();
        }
        return configuration;
    }

    // 刷新hbase表格,将memstore数据落盘到hdfs中
    public static void refreshTable(String hbaseTableName) {
        if (configuration == null || connection == null || admin == null) {
            init();
        }
        try {
            admin.flush(TableName.valueOf(hbaseTableName));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
