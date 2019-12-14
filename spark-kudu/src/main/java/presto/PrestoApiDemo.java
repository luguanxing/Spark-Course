package presto;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class PrestoApiDemo {

    public static void main(String[] args) throws Exception {
        // 获取connection，注意presto用户名可以随意
        String url = "jdbc:presto://127.0.0.1:9999";
        Properties properties = new Properties();
        properties.setProperty("user", "presto");
        Connection connection = DriverManager.getConnection(url, properties);

        // 执行statement
        Statement statement = connection.createStatement();
        ResultSet result = statement.executeQuery("SELECT t1.word, t1.wordcount, t2.name, t2.age FROM mysql.test.tbl_wordcount t1 INNER JOIN redis.presto_redis_schema.presto_redis_table t2 ON t1.word != t2.name");

        // 查询结果
        while (result.next()) {
            String word = result.getString("word");
            String wordcount = result.getString("wordcount");
            String name = result.getString("name");
            String age = result.getString("age");
            String resultLine = String.format("{word=%s, wordcount=%s, name=%s, age=%s}", word, wordcount, name, age);
            System.out.println(resultLine);
        }
    }

}
