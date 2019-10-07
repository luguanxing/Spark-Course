package log2flume2kafka2spark.domain;

public class ClickLogRowKey {

    String dayCourseId;

    Long clickCount;

    public ClickLogRowKey(String dayCourseId, Long clickCount) {
        this.dayCourseId = dayCourseId;
        this.clickCount = clickCount;
    }

    public String getDayCourseId() {
        return dayCourseId;
    }

    public void setDayCourseId(String dayCourseId) {
        this.dayCourseId = dayCourseId;
    }

    public Long getClickCount() {
        return clickCount;
    }

    public void setClickCount(Long clickCount) {
        this.clickCount = clickCount;
    }
}
