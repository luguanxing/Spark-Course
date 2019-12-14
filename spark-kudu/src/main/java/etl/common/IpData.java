package etl.common;

public class IpData {

    long start;
    long end;
    String province;
    String city;
    String isp;

    public IpData(long start, long end, String province, String city, String isp) {
        this.start = start;
        this.end = end;
        this.province = province;
        this.city = city;
        this.isp = isp;
    }

    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public long getEnd() {
        return end;
    }

    public void setEnd(long end) {
        this.end = end;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getIsp() {
        return isp;
    }

    public void setIsp(String isp) {
        this.isp = isp;
    }

}
