package org.immunizer.acquisition;

import org.apache.kafka.common.serialization.Deserializer;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;;

public class InvocationDeserializer implements Deserializer<JsonObject> {

    public InvocationDeserializer() {}
    
    @Override
    public JsonObject deserialize(String topic, byte[] bytes) {
        return JsonParser.parseString(new String(bytes)).getAsJsonObject();
    }
}