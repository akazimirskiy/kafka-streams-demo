package ru.kazimir.kafka.message;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

public class MessageDataDeserializer implements Deserializer<MessageData> {
    ObjectMapper objectMapper = new ObjectMapper();
    @Override
    public MessageData deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(data, StreamMessageImpl.class).getMessageData();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
