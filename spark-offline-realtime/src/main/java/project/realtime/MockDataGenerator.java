package project.realtime;

import com.google.gson.Gson;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;

public class MockDataGenerator {

    private static Random random = new Random();

    private static FastDateFormat fdf = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

    private static Gson gson = new Gson();

    private static String generateData() {
        Map map = new HashMap<>();
        map.put("orderid", UUID.randomUUID().toString());
        map.put("time", fdf.format(new Date().getTime()));
        map.put("userid", random.nextInt(1000));
        map.put("courseid", random.nextInt(500));
        map.put("fee", random.nextInt(400));
        map.put("payed", random.nextInt(2));
        String json = gson.toJson(map);
        return json;
    }

    private static KafkaProducer getKafkaProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.kafkaBrokerList);
        properties.put(ProducerConfig.ACKS_CONFIG, "1");
        return new KafkaProducer(properties);
    }

    public static void main(String[] args) {

        // 模拟自动发送数据
        new Thread(() -> {
            KafkaProducer kafkaProducer = getKafkaProducer();
            while (true) {
                kafkaProducer.send(new ProducerRecord<>(AppConfig.kafkaTopic, generateData()));
                try {Thread.sleep(1000);} catch (Exception e) {}
            }
        }).start();

    }

}
