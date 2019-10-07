package imooc.log.analysis.dao;

import imooc.log.analysis.domain.DayVideoStat;
import imooc.log.analysis.utils.MysqlUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

public class DayVideoStatDao {

    public static void saveList(List<DayVideoStat> statList) {
        Connection connection = null;
        PreparedStatement pstmt = null;
        try {
            connection = MysqlUtil.getConnection();
            connection.setAutoCommit(false);
            String sql = "INSERT INTO day_video_topn_stat VALUES (?, ?, ?)";
            pstmt = connection.prepareStatement(sql);
            for (DayVideoStat stat : statList) {
                pstmt.setString(1, stat.getDay());
                pstmt.setLong(2, stat.getCms_id());
                pstmt.setLong(3, stat.getTimes());
                pstmt.addBatch();
            }
            pstmt.executeBatch();
            connection.commit();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            MysqlUtil.close(connection, pstmt);
        }
    }


    public static void deleteData(String day) {
        Connection connection = null;
        PreparedStatement pstmt = null;
        try {
            connection = MysqlUtil.getConnection();
            String sql = "DELETE FROM day_video_topn_stat WHERE day = ?";
            pstmt = connection.prepareStatement(sql);
            pstmt.setString(1, day);
            pstmt.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            MysqlUtil.close(connection, pstmt);
        }
    }

}
