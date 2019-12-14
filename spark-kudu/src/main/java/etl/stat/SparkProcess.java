package etl.stat;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public interface SparkProcess {

    void process(JavaSparkContext jsc, SparkSession sparkSession);

}
