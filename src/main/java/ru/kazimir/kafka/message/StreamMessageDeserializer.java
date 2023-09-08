package ru.kazimir.kafka.message;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

public class StreamMessageDeserializer implements Deserializer<StreamMessage> {

    @Override
    public StreamMessage deserialize(String topic, byte[] data) {
        return () -> {
            try {
                return (new ObjectMapper()).readValue(data, MessageData.class);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }
}
