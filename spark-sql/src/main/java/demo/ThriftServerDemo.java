package demo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class ThriftServerDemo {

    public static void main(String[] args) throws Exception {

        Class.forName("org.apache.hive.jdbc.HiveDriver");

        Connection connection = DriverManager.getConnection("jdbc:hive2://localhost:10000", "luguanxing", "");

        PreparedStatement pstmt = connection.prepareStatement("select * from hive_wordcount");

        ResultSet rs = pstmt.executeQuery();

        while (rs.next()) {
            System.out.println(rs.getString(1));
        }

        rs.close();
        pstmt.close();
        connection.close();

    }

}
