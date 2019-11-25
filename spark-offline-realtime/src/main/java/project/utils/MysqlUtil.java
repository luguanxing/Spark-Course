package project.utils;

import java.util.Properties;

public class MysqlUtil {

    public static Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty("user", "root");
        properties.setProperty("password", "root");
        properties.setProperty("driver", "com.mysql.jdbc.Driver");
        return properties;
    }

}
