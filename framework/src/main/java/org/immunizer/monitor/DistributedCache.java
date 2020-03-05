package org.immunizer.monitor;

/*import org.apache.ignite.spark.JavaIgniteContext;
import org.apache.ignite.spark.JavaIgniteRDD;*/
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.util.StatCounter;

import scala.Tuple2;

public class DistributedCache {

    /*private JavaIgniteContext<String, Double> igniteContext1;
    private JavaIgniteContext<String, Integer> igniteContext2;
    private JavaIgniteRDD<String, Double> stdevsRDD;
    private JavaIgniteRDD<String, Double> meansRDD;
    private JavaIgniteRDD<String, Integer> pathsRDD;
    private JavaIgniteRDD<String, Integer> splits1RDD;
    private JavaIgniteRDD<String, Integer> splits3RDD;*/

    public DistributedCache(JavaSparkContext sc) {
        /*igniteContext1 = new JavaIgniteContext<String, Double>(sc, "org/immunizer/ignite-cfg.xml");
        igniteContext2 = new JavaIgniteContext<String, Integer>(sc, "org/immunizer/ignite-cfg.xml");

        stdevsRDD = igniteContext1.fromCache("stdevsRDD");
        meansRDD = igniteContext1.fromCache("meansRDD");
        pathsRDD = igniteContext2.fromCache("pathsRDD");
        splits1RDD = igniteContext2.fromCache("splits1RDD");
        splits3RDD = igniteContext2.fromCache("splits3RDD");*/
    }

    public void saveModel(JavaRDD<String> model) {
        JavaPairRDD<String, StatCounter> numbersModel = model.filter(record -> record.startsWith("numbers_"))
                .mapToPair(record -> {
                    String key = record.substring(8, record.lastIndexOf('_'));
                    Double value = Double.valueOf(record.substring(record.lastIndexOf('_') + 1));
                    return new Tuple2<String, Double>(key, value);
                }).aggregateByKey(new StatCounter(), StatCounter::merge, StatCounter::merge);
        JavaPairRDD<String, Double> stdevsModel = numbersModel
                .mapToPair(stats -> new Tuple2<String, Double>(stats._1(), stats._2().stdev()));
        JavaPairRDD<String, Double> meansModel = numbersModel
                .mapToPair(stats -> new Tuple2<String, Double>(stats._1(), stats._2().mean()));

        JavaPairRDD<String, Integer> pathsModel = model.filter(record -> record.startsWith("paths_"))
                .mapToPair(record -> new Tuple2<String, Integer>(record.substring(6), 1)).reduceByKey((a, b) -> a + b);

        JavaPairRDD<String, Integer> splits1Model = model.filter(record -> record.startsWith("splits_1_"))
                .mapToPair(record -> new Tuple2<String, Integer>(record.substring(9), 1)).reduceByKey((a, b) -> a + b);

        JavaPairRDD<String, Integer> splits3Model = model.filter(record -> record.startsWith("splits_3_"))
                .mapToPair(record -> new Tuple2<String, Integer>(record.substring(9), 1)).reduceByKey((a, b) -> a + b);

        stdevsModel.foreach(entry -> {
            System.out.println("STDEV: " + entry._1() + ": " + entry._2());
        });
        meansModel.foreach(entry -> {
            System.out.println("MEAN: " + entry._1() + ": " + entry._2());
        });
        pathsModel.foreach(entry -> {
            System.out.println("PATH: " + entry._1() + ": " + entry._2());
        });
        splits1Model.foreach(entry -> {
            System.out.println("SPLIT1: " + entry._1() + ": " + entry._2());
        });
        splits3Model.foreach(entry -> {
            System.out.println("SPLIT3: " + entry._1() + ": " + entry._2());
        });

        /*stdevsRDD.savePairs(stdevsModel);
        meansRDD.savePairs(meansModel);
        pathsRDD.savePairs(pathsModel);
        splits1RDD.savePairs(splits1Model);
        splits3RDD.savePairs(splits3Model);*/
    }
}