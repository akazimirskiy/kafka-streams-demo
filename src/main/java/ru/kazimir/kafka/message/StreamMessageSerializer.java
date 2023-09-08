package ru.kazimir.kafka.message;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

public class StreamMessageSerializer implements Serializer<StreamMessage> {

    @Override
    public byte[] serialize(String topic, StreamMessage data) {
        try {
            return (new ObjectMapper()).writeValueAsBytes(data.getMessageData());
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
