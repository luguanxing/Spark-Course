package project.utils;

import cz.mallat.uasparser.OnlineUpdater;
import cz.mallat.uasparser.UASparser;
import cz.mallat.uasparser.UserAgentInfo;
import project.domain.UAInfo;

public class UAUtil {

    private static UASparser parser = null;

    static {
        try {
            parser = new UASparser(OnlineUpdater.getVendoredInputStream());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static UAInfo parseUserAgentInfo(String userAgentInfoString) {
        UAInfo uaInfo = new UAInfo();
        try {
            UserAgentInfo userAgentInfo = parser.parse(userAgentInfoString);
            if (userAgentInfo != null) {
                uaInfo.setBrowserName(userAgentInfo.getUaName());
                uaInfo.setBrowserVersion(userAgentInfo.getBrowserVersionInfo());
                uaInfo.setOsName(userAgentInfo.getOsFamily());
                uaInfo.setOsVersion(userAgentInfo.getOsName());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return uaInfo;
    }


    public static void main(String[] args) {
        UAInfo uaInfo = parseUserAgentInfo("Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; )");
        System.out.println(uaInfo);
    }

}
