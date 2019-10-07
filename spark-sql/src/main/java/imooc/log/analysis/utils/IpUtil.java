package imooc.log.analysis.utils;

import com.ggstar.util.ip.IpHelper;

public class IpUtil {

    public static String getIpRegion(String ip) {
        return IpHelper.findRegionByIp(ip);
    }

    public static void main(String[] args) {
        String ip = "58.30.15.255";
        String region = getIpRegion(ip);
        System.out.println(region);
    }

}
