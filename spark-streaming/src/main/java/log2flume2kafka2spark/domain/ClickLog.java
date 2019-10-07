package log2flume2kafka2spark.domain;

import java.io.Serializable;

public class ClickLog implements Serializable {

    String ip;

    Long ts;

    Integer courseId;

    Integer statusCode;

    String referer;

    public ClickLog(String ip, Long ts, Integer courseId, Integer statusCode, String referer) {
        this.ip = ip;
        this.ts = ts;
        this.courseId = courseId;
        this.statusCode = statusCode;
        this.referer = referer;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public Integer getCourseId() {
        return courseId;
    }

    public void setCourseId(Integer courseId) {
        this.courseId = courseId;
    }

    public Integer getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(Integer statusCode) {
        this.statusCode = statusCode;
    }

    public String getReferer() {
        return referer;
    }

    public void setReferer(String referer) {
        this.referer = referer;
    }

    @Override
    public String toString() {
        return "ClickLog{" +
                "ip='" + ip + '\'' +
                ", ts=" + ts +
                ", courseId=" + courseId +
                ", statusCode=" + statusCode +
                ", referer='" + referer + '\'' +
                '}';
    }
}
