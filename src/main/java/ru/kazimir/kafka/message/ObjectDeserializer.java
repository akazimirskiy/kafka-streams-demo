package ru.kazimir.kafka.message;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

public class ObjectDeserializer<T> implements Deserializer<T> {
    private ObjectMapper objectMapper = new ObjectMapper();
    private Class<T> destinationClass;
    public ObjectDeserializer(Class<T> destinationClass) {

        this.destinationClass = destinationClass;
    }
    @Override
    public T deserialize(String s, byte[] bytes) {
        try {
            return objectMapper.readValue(bytes, destinationClass);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
