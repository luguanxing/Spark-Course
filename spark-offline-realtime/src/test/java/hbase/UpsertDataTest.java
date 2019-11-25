package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class UpsertDataTest {

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
    public void addData() throws Exception {
        Table table = connection.getTable(TableName.valueOf(hbaseTableName));
        Put put = new Put(Bytes.toBytes("van"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("30"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("birthday"), Bytes.toBytes("1990-01-01"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("company"), Bytes.toBytes("gym"));
        put.addColumn(Bytes.toBytes("address"), Bytes.toBytes("country"), Bytes.toBytes("japan"));
        put.addColumn(Bytes.toBytes("address"), Bytes.toBytes("city"), Bytes.toBytes("tokyo"));
        table.put(put);
    }

    @Test
    public void addDatas() throws Exception {
        Table table = connection.getTable(TableName.valueOf(hbaseTableName));
        List<Put> puts = new ArrayList<>();
        Put put1 = new Put(Bytes.toBytes("leijun"));
        put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("20"));
        put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("birthday"), Bytes.toBytes("2000-01-01"));
                put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("company"), Bytes.toBytes("xiaomi"));
        put1.addColumn(Bytes.toBytes("address"), Bytes.toBytes("country"), Bytes.toBytes("china"));
        put1.addColumn(Bytes.toBytes("address"), Bytes.toBytes("city"), Bytes.toBytes("beijing"));
        Put put2 = new Put(Bytes.toBytes("liuqiangdong"));
        put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("50"));
        put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("birthday"), Bytes.toBytes("2070-01-01"));
        put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("company"), Bytes.toBytes("jingdong"));
        put2.addColumn(Bytes.toBytes("address"), Bytes.toBytes("country"), Bytes.toBytes("china"));
        put2.addColumn(Bytes.toBytes("address"), Bytes.toBytes("city"), Bytes.toBytes("jiangsu"));
        puts.add(put1);
        puts.add(put2);
        table.put(puts);
    }

    @Test
    public void editData() throws Exception {
        Table table = connection.getTable(TableName.valueOf(hbaseTableName));
        Put put = new Put(Bytes.toBytes("van"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("50"));
        table.put(put);
    }

}