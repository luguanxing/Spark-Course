package project.domain;

import com.github.jarod.qqwry.IPZone;
import project.utils.IPUtil;
import project.utils.UAUtil;

public class LogInfo {

    String ip;

    String ipMainInfo;

    String ipSubInfo;

    String time;

    String method;

    String url;

    String protocal;

    int status;

    int bytesent;

    String referer;

    String userAgent;

    private String browserName;

    private String browserVersion;

    private String osName;

    private String osVersion;

    public LogInfo(String ip, String ipMainInfo, String ipSubInfo, String time, String method, String url, String protocal, int status, int bytesent, String referer, String userAgent, String browserName, String browserVersion, String osName, String osVersion) {
        this.ip = ip;
        this.ipMainInfo = ipMainInfo;
        this.ipSubInfo = ipSubInfo;
        this.time = time;
        this.method = method;
        this.url = url;
        this.protocal = protocal;
        this.status = status;
        this.bytesent = bytesent;
        this.referer = referer;
        this.userAgent = userAgent;
        this.browserName = browserName;
        this.browserVersion = browserVersion;
        this.osName = osName;
        this.osVersion = osVersion;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getIpMainInfo() {
        return ipMainInfo;
    }

    public void setIpMainInfo(String ipMainInfo) {
        this.ipMainInfo = ipMainInfo;
    }

    public String getIpSubInfo() {
        return ipSubInfo;
    }

    public void setIpSubInfo(String ipSubInfo) {
        this.ipSubInfo = ipSubInfo;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getProtocal() {
        return protocal;
    }

    public void setProtocal(String protocal) {
        this.protocal = protocal;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public int getBytesent() {
        return bytesent;
    }

    public void setBytesent(int bytesent) {
        this.bytesent = bytesent;
    }

    public String getReferer() {
        return referer;
    }

    public void setReferer(String referer) {
        this.referer = referer;
    }

    public String getUserAgent() {
        return userAgent;
    }

    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }

    public String getBrowserName() {
        return browserName;
    }

    public void setBrowserName(String browserName) {
        this.browserName = browserName;
    }

    public String getBrowserVersion() {
        return browserVersion;
    }

    public void setBrowserVersion(String browserVersion) {
        this.browserVersion = browserVersion;
    }

    public String getOsName() {
        return osName;
    }

    public void setOsName(String osName) {
        this.osName = osName;
    }

    public String getOsVersion() {
        return osVersion;
    }

    public void setOsVersion(String osVersion) {
        this.osVersion = osVersion;
    }

    @Override
    public String toString() {
        return "LogInfo{" +
                "ip='" + ip + '\'' +
                ", ipMainInfo='" + ipMainInfo + '\'' +
                ", ipSubInfo='" + ipSubInfo + '\'' +
                ", time='" + time + '\'' +
                ", method='" + method + '\'' +
                ", url='" + url + '\'' +
                ", protocal='" + protocal + '\'' +
                ", status=" + status +
                ", bytesent=" + bytesent +
                ", referer='" + referer + '\'' +
                ", userAgent='" + userAgent + '\'' +
                ", browserName='" + browserName + '\'' +
                ", browserVersion='" + browserVersion + '\'' +
                ", osName='" + osName + '\'' +
                ", osVersion='" + osVersion + '\'' +
                '}';
    }

    public static LogInfo convertToLogInfo(String log) {
        try {
            String ip = log.split(" ")[0];
            String time = log.substring(log.indexOf("["), log.indexOf("]") + 1);

            int index = log.indexOf("\"", 0) + 1;
            String request = log.substring(index, log.indexOf("\"", index));
            String[] requestInfos = request.split(" ");
            String method = requestInfos[0];
            String url = requestInfos[1];
            String protocal = requestInfos[2];

            index = log.indexOf("\"", index) + 1;
            String statusBytesent = log.substring(index, log.indexOf("\"", index));
            String[] statusBytesentInfos = statusBytesent.split(" ");
            int status = Integer.parseInt(statusBytesentInfos[1]);
            int bytesent = Integer.parseInt(statusBytesentInfos[2]);

            index = log.indexOf("\"", index) + 1;
            index = log.indexOf("\"", index) + 1;
            index = log.indexOf("\"", index) + 1;
            String referer = log.substring(index, log.indexOf("\"", index));

            index = log.indexOf("\"", index) + 1;
            index = log.indexOf("\"", index) + 1;
            String userAgent = log.substring(index, log.indexOf("\"", index));

            IPZone ipInfo = IPUtil.getIpInfo(ip);
            UAInfo uaInfo = UAUtil.parseUserAgentInfo(userAgent);
            LogInfo logInfo = new LogInfo(ip, ipInfo.getMainInfo(), ipInfo.getSubInfo(), time, method, url, protocal, status, bytesent, referer, userAgent, uaInfo.getBrowserName(), uaInfo.getBrowserVersion(), uaInfo.getOsName(), uaInfo.getOsVersion());
            return logInfo;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void main(String[] args) {
        String log1 = "110.85.18.234 - - [30/Jan/2019:00:00:21 +0800] \"GET /course/list?c=cb HTTP/1.1\" 200 12800 \"www.imooc.com\" \"https://www.imooc.com/course/list?c=data\" - \"Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36 SE 2.X MetaSr 1.0\" \"-\" 10.100.16.243:80 200 0.172 0.172";
        String log2 = "113.77.139.245 - - [30/Jan/2019:00:00:24 +0800] \"GET /index/getstarlist HTTP/1.1\" 200 2910 \"www.imooc.com\" \"https://www.imooc.com/\" - \"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3642.0 Safari/537.36\" \"-\" 10.100.16.243:80 200 0.008 0.008";
        String log3 = "1.202.65.40 - - [30/Jan/2019:00:00:26 +0800] \"GET /static/component/logic/common/common.js?v=201901251938 HTTP/1.1\" 200 3532 \"www.imooc.com\" \"https://www.imooc.com/qadetail/269497\" - \"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36\" \"-\" 10.100.137.42:80 200 0.002 0.002";
        LogInfo logInfo1 = convertToLogInfo(log1);
        LogInfo logInfo2 = convertToLogInfo(log2);
        LogInfo logInfo3 = convertToLogInfo(log3);
        System.out.println(logInfo1);
        System.out.println(logInfo2);
        System.out.println(logInfo3);
    }

}
