package project.realtime;

import com.typesafe.config.ConfigFactory;

public class AppConfig {

    private static com.typesafe.config.Config config = null;

    public static String kafkaTopic;
    public static String kafkaGroupId;
    public static String kafkaBrokerList;
    public static String redisHost;
    public static int redisPort;

    static {
        config = ConfigFactory.load();
        kafkaTopic = config.getString("kafka.topic");
        kafkaGroupId = config.getString("kafka.group.id");
        kafkaBrokerList = config.getString("kafka.broker.list");
        redisHost = config.getString("redis.host");
        redisPort = config.getInt("redis.port");
    }

    public static void main(String[] args) {
        System.out.println(kafkaTopic);
        System.out.println(kafkaGroupId);
        System.out.println(kafkaBrokerList);
        System.out.println(redisHost);
        System.out.println(redisPort);
    }

}
