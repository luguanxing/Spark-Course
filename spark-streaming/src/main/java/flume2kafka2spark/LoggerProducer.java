package flume2kafka2spark;

import org.apache.log4j.Logger;

public class LoggerProducer {

    private static Logger logger = Logger.getLogger(LoggerProducer.class);

    public static void main(String[] args) throws Exception {

        int value = 0;
        while (true) {
            logger.info("value = " + value++);
            Thread.sleep(1000);
        }

    }

}
