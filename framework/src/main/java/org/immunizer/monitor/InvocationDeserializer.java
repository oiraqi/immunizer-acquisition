package org.immunizer.monitor;

import org.apache.kafka.common.serialization.Deserializer;

import java.io.StringReader;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class InvocationDeserializer implements Deserializer<JsonObject> {

    public InvocationDeserializer() {}
    
    @Override
    public JsonObject deserialize(String topic, byte[] bytes) {
        return JsonParser.parseReader(new StringReader(new String(bytes))).getAsJsonObject();
    }
}