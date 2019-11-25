package project.utils;

import com.github.jarod.qqwry.IPZone;
import com.github.jarod.qqwry.QQWry;

public class IPUtil {

    private static QQWry qqwry = null;

    static {
        try {
            qqwry = new QQWry();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static IPZone getIpInfo(String ip) {
        return qqwry.findIP(ip);
    }

    public static void main(String[] args) {
        String ip = "58.248.226.9";
        IPZone ipzone = getIpInfo(ip);
        System.out.printf("%s, %s", ipzone.getMainInfo(), ipzone.getSubInfo());
    }

}
