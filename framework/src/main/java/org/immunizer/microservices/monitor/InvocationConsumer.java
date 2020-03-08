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
    private static final String GROUP_ID = "MONITORING_GROUP";
    private static final String TOPIC_PATTERN = "Invocations_[0-9]+";
    private static int SIZE = 0;

    public InvocationConsumer() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.setProperty("group.id", GROUP_ID);
        // props.put("auto.offset.reset", "earliest");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        // props.put("session.timeout.ms", "30000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.immunizer.microservices.monitor.InvocationDeserializer");

        consumer = new KafkaConsumer<String, byte[]>(props);
        consumer.subscribe(Pattern.compile(TOPIC_PATTERN));
        // consumer.seekToBeginning(Collections.emptyList());
    }

    public Vector<byte[]> poll (Duration timeout) {
        /**
         * Make sure to poll at least SIZE records. Otherwise poll all records
         * from beginning offsets.
         */
        Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(consumer.assignment());
        consumer.endOffsets(consumer.assignment()).forEach((partition, endOffset) -> {
            System.out.println(partition.topic() + " : " + endOffset);
            if (endOffset - consumer.position(partition) < SIZE) {
                if (endOffset - SIZE > beginningOffsets.get(partition)) {
                    consumer.seek(partition, endOffset - SIZE);
                } else {
                    consumer.seek(partition, beginningOffsets.get(partition));
                }
            }
        });

        /**
         * Append a timestamp to each record as an id, since each record may
         * be polled and processed more than once.
         */
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