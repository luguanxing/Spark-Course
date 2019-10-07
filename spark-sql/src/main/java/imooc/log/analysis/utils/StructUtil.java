package imooc.log.analysis.utils;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class StructUtil {

    public static StructType getStructType() {
        List<StructField> fields = new ArrayList() {};
        fields.add(DataTypes.createStructField("day", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("time", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("ip", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("url", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("city", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("traffic", DataTypes.LongType, true));
        fields.add(DataTypes.createStructField("cmsType", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("cmsId", DataTypes.LongType, true));
        return DataTypes.createStructType(fields);
    }

    // 示例：2016-11-10 00:01:02	117.35.88.11	http://www.imooc.com/code/1852	2345
    public static Row parseAccessInfo(String accessInfo) {
        try {
            // 获取基本信息
            String[] infos = accessInfo.split("\t");
            String time = infos[0];
            String day = infos[0].split(" ")[0];
            String ip = infos[1];
            String url = infos[2];
            Long traffic = 0l;
            String city = "";
            try {
                traffic = Long.parseLong(infos[3]);
                city = IpUtil.getIpRegion(ip);
            } catch (Exception e) {}

            // 获取cms信息
            String domain = "http://www.imooc.com";
            String cmsType = "";
            Long cmsId = 0l;
            if (url.contains(domain)) {
                String[] cmsInfos = url.substring(url.indexOf(domain) + domain.length()).split("/");
                if (cmsInfos.length > 1) {
                    cmsType = cmsInfos[1];
                    try {
                        cmsId = Long.parseLong(cmsInfos[2]);
                    } catch (Exception e) {}
                }
            }

            // 返回Row
            return RowFactory.create(day, time, ip, url, city, traffic, cmsType, cmsId);
        } catch (Exception e) {
            e.printStackTrace();
            return RowFactory.create();
        }
    }

}
