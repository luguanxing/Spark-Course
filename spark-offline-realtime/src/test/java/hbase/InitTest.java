package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class InitTest {

    Connection connection = null;

    Admin admin = null;

    @Before
    public void start() {
        try {
            Configuration configuration = new Configuration();
            configuration.set("hbase.rootdir", "hdfs://127.0.0.1:8020/hbase");
            configuration.set("hbase.zookeeper.quorum", "127.0.0.1:2181");
            connection = ConnectionFactory.createConnection(configuration);
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
    public void testHbase() {

    }


}
