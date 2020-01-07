package org.immunizer.acquisition;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class FeatureRecordProducer {

    private static FeatureRecordProducer singleton;
    private KafkaProducer<String, FeatureRecord> producer;
    private static final String BOOTSTRAP_SERVERS = "kafka-container:9092";
    private static final String TOPIC = "FeatureRecords";

    private FeatureRecordProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.immunizer.acquisition.FeatureRecordSerializer");
        producer = new KafkaProducer<String, FeatureRecord>(props);
    }

    public static FeatureRecordProducer getSingleton() {
        if (singleton == null) {
            singleton = new FeatureRecordProducer();
        }
        return singleton;
    }

    public void send(FeatureRecord featureRecord) {
        producer.send(new ProducerRecord<String, FeatureRecord>(TOPIC, featureRecord));
    }
}