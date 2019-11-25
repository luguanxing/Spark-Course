package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class GetByRowkeyTest {

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
    public void getKeyData() throws Exception {
        Table table = connection.getTable(TableName.valueOf(hbaseTableName));
        Get get = new Get("van".getBytes());
        Result result = table.get(get);
        printResult(result);
    }

    @Test
    public void getKeyColData() throws Exception {
        Table table = connection.getTable(TableName.valueOf(hbaseTableName));
        Get get = new Get("van".getBytes());
        get.addColumn("info".getBytes(), "age".getBytes());
        get.addColumn("info".getBytes(), "birthday".getBytes());
        get.addColumn("info".getBytes(), "company".getBytes());
        Result result = table.get(get);
        printResult(result);
    }

    private void printResult(Result result) {
        for (Cell cell : result.rawCells()) {
            String rowkey = Bytes.toString(result.getRow());
            String columnFamily = Bytes.toString(CellUtil.cloneFamily(cell));
            String column = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            long ts = cell.getTimestamp();
            System.out.println(rowkey + "\t" + columnFamily + "\t" + column + "\t" + value + "\t" + ts);
        }
    }

}