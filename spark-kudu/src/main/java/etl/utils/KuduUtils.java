package etl.utils;

import org.apache.kudu.Schema;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

public class KuduUtils {

    public static Dataset getOdsDF(SparkSession sparkSession) {
        Dataset<Row> kuduDF = sparkSession.read()
                .format("org.apache.kudu.spark.kudu")
                .option("kudu.master", "vmware-centos")
                .option("kudu.table", "ods")
                .load();
        return kuduDF;
    }

    public static void saveDataToKudu(Dataset dataFrame, String tableName, Schema schema, String partitionKeys) {
        createTable(tableName, schema, partitionKeys);
        dataFrame.write()
                .mode(SaveMode.Append)
                .format("org.apache.kudu.spark.kudu")
                .option("kudu.master", "vmware-centos")
                .option("kudu.table", tableName)
                .save();
    }

    private static void createTable(String tableName, Schema schema, String partitionKeys) {
        try {
            KuduClient client = new KuduClient.KuduClientBuilder("vmware-centos").build();
            if (client.tableExists(tableName)) {
                client.deleteTable(tableName);
            }
            CreateTableOptions cto = new CreateTableOptions();
            List<String> partitionKeysList = Arrays.asList(partitionKeys.split(","));
            cto.addHashPartitions(partitionKeysList, 3);
            cto.setNumReplicas(1);
            client.createTable(tableName, schema, cto);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
