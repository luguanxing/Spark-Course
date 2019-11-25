package project.domain;

public class UAInfo {

    private String browserName;

    private String browserVersion;

    private String osName;

    private String osVersion;

    public String getBrowserName() {
        return browserName;
    }

    public void setBrowserName(String browserName) {
        this.browserName = (browserName == null ? "" : browserName);
    }

    public String getBrowserVersion() {
        return browserVersion;
    }

    public void setBrowserVersion(String browserVersion) {
        this.browserVersion = (browserVersion == null ? "" : browserVersion);
    }

    public String getOsName() {
        return osName;
    }

    public void setOsName(String osName) {
        this.osName = (osName == null ? "" : osName);
    }

    public String getOsVersion() {
        return osVersion;
    }

    public void setOsVersion(String osVersion) {
        this.osVersion = (osVersion == null ? "" : osVersion);
    }

    @Override
    public String toString() {
        return "UAInfo{" +
                "browserName='" + browserName + '\'' +
                ", browserVersion='" + browserVersion + '\'' +
                ", osName='" + osName + '\'' +
                ", osVersion='" + osVersion + '\'' +
                '}';
    }

}
