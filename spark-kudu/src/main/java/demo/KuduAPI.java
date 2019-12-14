package demo;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class KuduAPI {

    public static void main(String[] args) throws Exception {
        // 初始化
        String KUDU_MASTERS_URL = "vmware-centos";
        KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTERS_URL).build();
        String tableName = "test";
        String tableName2 = "test2";

        // 增删改查
        createTable(client, tableName);
        addData(client, tableName);
        queryTable(client, tableName);
        modifyTableName(client, tableName, tableName2);
        upsertTable(client, tableName2);
        queryTable(client, tableName2);
        deleteTable(client, tableName2);

        // 关闭client
        client.close();
    }

    public static void createTable(KuduClient client, String tableName) throws Exception {
        // 设置表字段Schema
        List<ColumnSchema> columns = new ArrayList<>(2);
        columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.STRING).key(true).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("value", Type.STRING).nullable(true).build());
        Schema schema = new Schema(columns);

        // 设置建表选项，包括分区字段和分区数，副本数
        CreateTableOptions cto = new CreateTableOptions();
        List<String> hashKeys = Arrays.asList("key".split(","));
        cto.addHashPartitions(hashKeys, 8);
        cto.setNumReplicas(1);

        // 创建表
        client.createTable(tableName, schema, cto);
        System.out.println("Created table " + tableName);
    }

    public static void addData(KuduClient client, String tableName) throws Exception {
        // 获取数据表
        KuduTable table = client.openTable(tableName);

        // 创建session
        KuduSession session = client.newSession();

        // 添加数据
        for (int i = 1; i <= 10; i++) {
            Insert insert = table.newInsert();
            PartialRow row = insert.getRow();
            row.addString("key", i + "");
            row.addString("value", "value_" + i);
            session.apply(insert);
        }
    }

    public static void deleteTable(KuduClient client, String tableName) throws Exception {
        // 删除表
        client.deleteTable(tableName);
        System.out.println("Deleted table " + tableName);
    }

    private static void queryTable(KuduClient client, String tableName) throws Exception {
        // 获取数据表
        KuduTable table = client.openTable(tableName);

        // 创建scanner
        KuduScanner scanner = client.newScannerBuilder(table).build();

        // 遍历数据
        System.out.println();
        System.out.println("===============");
        while (scanner.hasMoreRows()) {
            RowResultIterator iterator = scanner.nextRows();
            while (iterator.hasNext()) {
                RowResult result = iterator.next();
                System.out.println(result.getString("key") + "," + result.getString("value"));
            }
        }
        System.out.println("===============");
        System.out.println();
    }

    private static void upsertTable(KuduClient client, String tableName) throws Exception {
        // 获取数据表
        KuduTable table = client.openTable(tableName);

        // 创建session
        KuduSession session = client.newSession();

        // 更新数据
        for (int i = 1; i <= 5; i++) {
            Upsert upsert = table.newUpsert();
            PartialRow row = upsert.getRow();
            row.addString("key", i + "");
            row.addString("value", "value_update_" + i);
            session.apply(upsert);
        }
    }

    public static void modifyTableName(KuduClient client, String oldName, String newName) throws Exception {
        // 设置新表属性
        AlterTableOptions options = new AlterTableOptions();
        options.renameTable(newName);

        // 修改表名
        client.alterTable(oldName, options);
        System.out.println("Rename table [" + oldName + "] as [" + newName + "]");
    }

}
