package org.immunizer.microservices.monitor;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.Consumer;

import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import java.util.regex.Pattern;

import java.time.Duration;

public class InvocationConsumer {

    private Consumer<String, byte[]> consumer;
    private static final String BOOTSTRAP_SERVERS = "kafka:9092";
    private static final String GROUP_ID = "Monitor";
    private static final String TOPIC_PATTERN = "Invocations/.+";
    private static int SIZE = 0;

    public InvocationConsumer() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.setProperty("group.id", GROUP_ID);
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        consumer = new KafkaConsumer<String, byte[]>(props);
        consumer.subscribe(Pattern.compile(TOPIC_PATTERN));
    }

    public Vector<byte[]> poll (Duration timeout) {        
        ConsumerRecords<String, byte[]> records = consumer.poll(timeout);
        Vector<byte[]> vector = new Vector<byte[]>();
        records.forEach(record -> {            
            vector.add(record.value());
            System.out.println(new String(record.value()));
        });
        
        return vector;
    }

    public void close() {
        consumer.close();
    }
}