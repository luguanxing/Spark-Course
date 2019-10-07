package imooc.log.analysis.utils;

import org.apache.commons.lang3.time.FastDateFormat;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class DateUtil {

    // 注意SimpleDateFormat线程不安全
    // private static SimpleDateFormat inputSdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
    private static FastDateFormat inputSdf = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);

    // private static SimpleDateFormat outputSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static FastDateFormat outputSdf = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

    // 转换日期格式
    public static String parseTimeStr(String timeStr) {
        try {
            String time = timeStr.substring(timeStr.indexOf("[") + 1, timeStr.lastIndexOf("]"));
            Date inputDate = inputSdf.parse(time);
            String output = outputSdf.format(inputDate);
            return output;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void main(String[] args) {
        System.out.println(parseTimeStr("[10/Nov/2016:00:01:02+0800]"));
    }

}
