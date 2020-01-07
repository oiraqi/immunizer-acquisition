package org.immunizer.acquisition;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.Consumer;

import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.time.Duration;

public class InvocationConsumer {

    private Consumer<String, Invocation> consumer;
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String GROUP_ID = "test";
    private static final String TOPIC = "Invocations";

    public InvocationConsumer () {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.setProperty("group.id", GROUP_ID);
        props.put("auto.offset.reset", "earliest"); 
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.immunizer.acquisition.InvocationDeserializer");

        consumer = new KafkaConsumer<String, Invocation>(props);
        Collection<String> topics = Collections.singletonList(TOPIC);        
        consumer.subscribe(topics);
        consumer.seekToBeginning(Collections.emptyList());
    }

    public ConsumerRecords<String, Invocation> poll (Duration timeout) {
        return consumer.poll(timeout);
    }

    public void close () {
        consumer.close();
    }
}