package etl.utils;

public class IpUtils {

    public static Long getIpLong(String ip) {
        Long num = 0L;
        try{
            ip = ip.replaceAll("[^0-9\\.]", "");
            String[] ips = ip.split("\\.");
            if (ips.length == 4){
                num = Long.parseLong(ips[0], 10) * 256L * 256L * 256L + Long.parseLong(ips[1], 10) * 256L * 256L + Long.parseLong(ips[2], 10) * 256L + Long.parseLong(ips[3], 10);
                num = num >>> 0;
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        return num;
    }

    public static void main(String[] args) {
        Long ipLong = getIpLong("182.91.190.221");
        System.out.println(ipLong);
    }

}
