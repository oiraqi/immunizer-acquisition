package org.immunizer.monitor;

import java.util.Vector;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

import java.time.Duration;

public class MonitorApplication {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("Monitor").setMaster("spark://spark-master:7077");
        JavaSparkContext sc = new JavaSparkContext(conf);
        InvocationConsumer consumer = new InvocationConsumer();
        DistributedCache cache = new DistributedCache(sc);

        try {
            while (true) {
                Vector<byte[]> records = consumer.poll(Duration.ofSeconds(60));
                JavaRDD<byte[]> rdd = sc.parallelize(records);
                JavaRDD<String> model = rdd.flatMap(new ModelMapper());
                cache.saveModel(model);
            }
        } finally {
            consumer.close();
            sc.close();
        }
    }

}