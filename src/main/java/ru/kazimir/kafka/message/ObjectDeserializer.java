package ru.kazimir.kafka.message;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

public class ObjectDeserializer<T> implements Deserializer<T> {
    private ObjectMapper objectMapper = new ObjectMapper();
    
    @Override
    public T deserialize(String s, byte[] bytes) {
        JavaType type = objectMapper.constructType(T);
        return objectMapper.readValue(bytes, T);
    }
}
