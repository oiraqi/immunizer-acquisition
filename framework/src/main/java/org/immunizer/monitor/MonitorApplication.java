package org.immunizer.monitor;

import java.util.Vector;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.StatCounter;

import scala.Tuple2;
import scala.Tuple3;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

import java.time.Duration;

import com.google.gson.JsonObject;

public class MonitorApplication {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("Test").setMaster("master");
        JavaSparkContext sc = new JavaSparkContext(conf);
        InvocationConsumer consumer = new InvocationConsumer();
        /*
         * FeatureExtractor extractor = FeatureExtractor.getSingleton();
         * FeatureRecordProducer producer = new FeatureRecordProducer();
         */

        try {
            while (true) {
                Vector<JsonObject> records = consumer.poll(Duration.ofSeconds(300));
                JavaRDD<JsonObject> rdd = sc.parallelize(records);

                JavaRDD<String> model = rdd.flatMap(new ModelMapper());

                JavaRDD<Tuple3<String, Double, Double>> numbersModel = model
                        .filter(record -> record.startsWith("numbers_")).mapToPair(record -> {
                            String key = record.substring(0, record.lastIndexOf('_'));
                            Double value = Double.valueOf(record.substring(record.lastIndexOf('_') + 1));
                            return new Tuple2<String, Double>(key, value);
                        }).aggregateByKey(new StatCounter(), StatCounter::merge, StatCounter::merge)
                        .map(stats -> new Tuple3<String, Double, Double>(stats._1(), 
                            stats._2().stdev(), stats._2().mean()));

                JavaPairRDD<String, Integer> pathsModel = model.filter(record -> record.startsWith("paths_"))
                        .mapToPair(record -> new Tuple2<String, Integer>(record, 1)).reduceByKey((a, b) -> a + b);

                JavaPairRDD<String, Integer> splits1Model = model.filter(record -> record.startsWith("splits_1_"))
                        .mapToPair(record -> new Tuple2<String, Integer>(record, 1)).reduceByKey((a, b) -> a + b);

                JavaPairRDD<String, Integer> splits3Model = model.filter(record -> record.startsWith("splits_3_"))
                        .mapToPair(record -> new Tuple2<String, Integer>(record, 1)).reduceByKey((a, b) -> a + b);
            }
        } finally {
            consumer.close();
            sc.close();
        }
    }

}