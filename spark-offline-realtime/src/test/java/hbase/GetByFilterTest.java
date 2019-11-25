package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class GetByFilterTest {

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
    public void filterData() throws Exception {
        Table table = connection.getTable(TableName.valueOf(hbaseTableName));
        Scan scan = new Scan();
        // 对rowkey正则匹配
        // scan.setFilter(new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("^*a^*")));
        // 对rowkey前缀过滤
        // scan.setFilter(new PrefixFilter("l".getBytes()));
        // 设定批量过滤条件，可以是与或的关系
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        filterList.addFilter(new PrefixFilter("l".getBytes()));
        filterList.addFilter(new PrefixFilter("v".getBytes()));
        scan.setFilter(filterList);
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            printResult(result);
            System.out.println();
        }
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