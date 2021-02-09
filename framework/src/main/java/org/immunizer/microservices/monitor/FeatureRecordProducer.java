package org.immunizer.microservices.monitor;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.Serializable;

public class FeatureRecordProducer implements Serializable {

    private static final long serialVersionUID = 1876876L;

    private KafkaProducer<String, FeatureRecord> producer;
    private static final String BOOTSTRAP_SERVERS = "localhost:29092";
    private static final String TOPIC = "FeatureRecords";

    public FeatureRecordProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.immunizer.microservices.monitor.FeatureRecordSerializer");
        producer = new KafkaProducer<String, FeatureRecord>(props);
    }

    public void send(FeatureRecord featureRecord) {
        producer.send(new ProducerRecord<String, FeatureRecord>(TOPIC, featureRecord.getCallStackId(), "", featureRecord));
    }
}