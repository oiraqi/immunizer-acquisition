package org.immunizer.acquisition;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;

public class AcquisitionApplication {

    public static void main(String[] args) {
        
        InvocationConsumer consumer = new InvocationConsumer();        
        FeatureExtractor extractor = FeatureExtractor.getSingleton();
        FeatureRecordProducer producer = new FeatureRecordProducer();

        try {            
            while (true) {
                ConsumerRecords<String, Invocation> records = consumer.poll(Duration.ofSeconds(15));
                for (ConsumerRecord<String, Invocation> record : records){
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