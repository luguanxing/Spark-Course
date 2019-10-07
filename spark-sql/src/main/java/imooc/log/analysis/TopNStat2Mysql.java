package imooc.log.analysis;

import imooc.log.analysis.dao.DayVideoStatDao;
import imooc.log.analysis.domain.DayVideoStat;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

// 统计topN
public class TopNStat2Mysql {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("TopNStat2Mysql")
                .master("local[2]")
                .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")  // 关闭分区类型自动推断
                .getOrCreate();

        // 读取日志，解析数据RDD
        String inputPath = "/Users/luguanxing/data/access.20161111.all/output_row";
        Dataset<Row> accessDF = sparkSession.read().parquet(inputPath);

        // 验证数据
        accessDF.printSchema();
        accessDF.show();
        System.out.println(accessDF.count());

        // 统计topN
        accessDF.createOrReplaceTempView("tbl_access");
        Dataset<Row> videoTopN = sparkSession.sql(
                "SELECT day, cmsId, count(1) AS cnt FROM tbl_access " +
                        "WHERE day='2016-11-10' AND cmsType='video' " +
                        "GROUP BY day, cmsId " +
                        "ORDER BY cnt DESC");
        videoTopN.show();

        // 统计结果入库Mysql
        try {
            DayVideoStatDao.deleteData("2016-11-10");
            videoTopN.foreachPartition(
                    partiitonOfRecords -> {
                        List<DayVideoStat> statList = new ArrayList<>();
                        while (partiitonOfRecords.hasNext()) {
                            Row row = partiitonOfRecords.next();
                            String day = row.getAs("day");
                            Long cmsId = row.getAs("cmsId");
                            Long times = row.getAs("cnt");
                            DayVideoStat stat = new DayVideoStat(day, cmsId, times);
                            statList.add(stat);
                        }
                        DayVideoStatDao.saveList(statList);
                    }
            );
        } catch (Exception e) {
            e.printStackTrace();
        }

        sparkSession.stop();
    }

}
