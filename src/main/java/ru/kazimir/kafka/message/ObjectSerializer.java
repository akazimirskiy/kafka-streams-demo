package ru.kazimir.kafka.message;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

public class ObjectSerializer<T> implements Serializer<T> {
    private ObjectMapper objectMapper = new ObjectMapper();
    @Override
    public byte[] serialize(String s, T t) {
        try {
            return objectMapper.writeValueAsBytes(t);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
