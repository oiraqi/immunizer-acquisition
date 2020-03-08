package org.immunizer.microservices.monitor;

import org.apache.kafka.common.serialization.Serializer;
import com.google.gson.Gson;

public class FeatureRecordSerializer implements Serializer<FeatureRecord> {

    private Gson gson = new Gson();

    public FeatureRecordSerializer() {}
    
    @Override
    public byte[] serialize(String topic, FeatureRecord featureRecord) {
        return gson.toJson(featureRecord).getBytes();
    }
}