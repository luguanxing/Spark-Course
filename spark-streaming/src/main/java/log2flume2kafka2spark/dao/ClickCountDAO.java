package log2flume2kafka2spark.dao;

import log2flume2kafka2spark.domain.ClickLogRowKey;
import log2flume2kafka2spark.utils.HBaseUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

public class ClickCountDAO {

    private static String tableName = "course_clickcount";

    private static String cf = "info";

    private static String qualifer = "click_count";

    public static void save(List<ClickLogRowKey> clickLogRowKeys) {
        try {
            HTable table = HBaseUtil.getHBaseUtilInstance().getTable(tableName);
            for (ClickLogRowKey clickLogRowKey : clickLogRowKeys) {
                table.incrementColumnValue(
                        clickLogRowKey.getDayCourseId().getBytes(),
                        cf.getBytes(),
                        qualifer.getBytes(),
                        clickLogRowKey.getClickCount()
                );
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Long get(String dayCourseId) {
        try {
            HTable table = HBaseUtil.getHBaseUtilInstance().getTable(tableName);
            Get get = new Get(dayCourseId.getBytes());
            byte[] bytes = table.get(get).getValue(cf.getBytes(), qualifer.getBytes());
            if (bytes == null) {
                return 0l;
            } else {
                long value = Bytes.toLong(bytes);
                return value;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return -1l;
        }
    }

    public static void main(String[] args) {
        List<ClickLogRowKey> list = new ArrayList<>();
        ClickLogRowKey clickLogRowKey1 = new ClickLogRowKey("20171111_1", 1L);
        ClickLogRowKey clickLogRowKey2 = new ClickLogRowKey("20171111_2", 2L);
        ClickLogRowKey clickLogRowKey3 = new ClickLogRowKey("20171111_3", 3L);
        list.add(clickLogRowKey1);
        list.add(clickLogRowKey2);
        list.add(clickLogRowKey3);

        ClickCountDAO clickCountDAO = new ClickCountDAO();
        clickCountDAO.save(list);

        System.out.println(clickCountDAO.get("20171111_1"));
        System.out.println(clickCountDAO.get("20171111_2"));
        System.out.println(clickCountDAO.get("20171111_3"));
    }

}
