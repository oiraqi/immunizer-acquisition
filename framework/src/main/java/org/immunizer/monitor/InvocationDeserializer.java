package org.immunizer.monitor;

import org.apache.kafka.common.serialization.Deserializer;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class InvocationDeserializer implements Deserializer<JsonObject> {

    JsonParser parser;

    public InvocationDeserializer() {
        parser = new JsonParser();
    }
    
    @Override
    public JsonObject deserialize(String topic, byte[] bytes) {
        // return JsonParser.parseString(new String(bytes)).getAsJsonObject();
        return parser.parse(new String(bytes)).getAsJsonObject();
    }
}