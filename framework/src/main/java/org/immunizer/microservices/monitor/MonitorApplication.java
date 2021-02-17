package org.immunizer.microservices.monitor;

import java.util.Vector;
import java.time.Duration;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

public class MonitorApplication {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Monitor").setMaster("spark://spark-master:7077");
        JavaSparkContext sc = new JavaSparkContext(conf);
        InvocationConsumer consumer = new InvocationConsumer();
        DistributedCache cache = new DistributedCache(sc);

        try {
            while (true) {
                Vector<byte[]> records = consumer.poll(Duration.ofSeconds(60));
                JavaRDD<String> model = sc.parallelize(records).flatMap(new ModelMapper()).filter(record -> record != null);
                cache.saveModel(model);
            }
        } finally {
            consumer.close();
            sc.close();
        }
    }
}