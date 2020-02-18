package org.immunizer.monitor;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;

import com.google.gson.JsonObject;

public class MonitorApplication {

    public static void main(String[] args) {
        
        InvocationConsumer consumer = new InvocationConsumer();        
        FeatureExtractor extractor = FeatureExtractor.getSingleton();
        FeatureRecordProducer producer = new FeatureRecordProducer();

        try {            
            while (true) {
                ConsumerRecords<String, JsonObject> records = consumer.poll(Duration.ofSeconds(15));
                for (ConsumerRecord<String, JsonObject> record : records){
                    FeatureRecord featureRecord = extractor.extract(record.value());
                    if (featureRecord != null) {
                        producer.send(featureRecord);
                    }
                }
            }
        } finally {
            consumer.close();
        }
    }

}