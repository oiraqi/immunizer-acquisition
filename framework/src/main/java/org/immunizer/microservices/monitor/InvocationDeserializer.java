package org.immunizer.microservices.monitor;

import org.apache.kafka.common.serialization.Deserializer;

public class InvocationDeserializer implements Deserializer<byte[]> {

    public InvocationDeserializer() { }
    
    @Override
    public byte[] deserialize(String topic, byte[] bytes) {
        return bytes;
    }
}