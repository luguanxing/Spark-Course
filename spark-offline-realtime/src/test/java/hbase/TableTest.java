package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TableTest {

    Connection connection = null;

    Admin admin = null;

    String hbaseTableName = "hbase_java";

    @Before
    public void start() {
        try {
            Configuration configuration = new Configuration();
            configuration.set("hbase.rootdir", "hdfs://127.0.0.1:8020/hbase");
            configuration.set("hbase.zookeeper.quorum", "127.0.0.1:2181");
            connection = ConnectionFactory.createConnection();
            admin = connection.getAdmin();
            Assert.assertNotNull(connection);
            Assert.assertNotNull(admin);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @After
    public void close() {
        try {
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void createTable() throws Exception {
        TableName tableName = TableName.valueOf(hbaseTableName);
        if (admin.tableExists(tableName)) {
            System.out.println(hbaseTableName + " 已经存在，创建失败...");
        } else {
            HTableDescriptor descriptor = new HTableDescriptor(tableName);
            descriptor.addFamily(new HColumnDescriptor("info"));
            descriptor.addFamily(new HColumnDescriptor("address"));
            admin.createTable(descriptor);
            System.out.println(hbaseTableName + " 创建成功...");
        }
    }

    @Test
    public void listTables() throws Exception {
        HTableDescriptor[] tables = admin.listTables();
        if (tables.length > 0) {
            for (HTableDescriptor table : tables) {
                System.out.println(table.getNameAsString());
                HColumnDescriptor[] columnFamilies = table.getColumnFamilies();
                for(HColumnDescriptor columnFamiliy : columnFamilies) {
                    System.out.println("\t" + columnFamiliy.getNameAsString());
                }
            }
        }
    }

}