package imooc.log.analysis.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class MysqlUtil {

    public static Connection getConnection() throws Exception {
        Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?user=root&password=root");
        return connection;
    }

    public static void close(Connection connection, PreparedStatement pstmt) {
        try {
            if (pstmt != null) {
                pstmt.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println(getConnection());
    }

}
