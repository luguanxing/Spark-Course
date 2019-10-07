package imooc.log.analysis.domain;

public class DayVideoStat {

    String day;

    Long cms_id;

    Long times;

    public DayVideoStat(String day, Long cms_id, Long times) {
        this.day = day;
        this.cms_id = cms_id;
        this.times = times;
    }

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    public Long getCms_id() {
        return cms_id;
    }

    public void setCms_id(Long cms_id) {
        this.cms_id = cms_id;
    }

    public Long getTimes() {
        return times;
    }

    public void setTimes(Long times) {
        this.times = times;
    }
}
