package ru.kazimir.kafka.message;

import org.apache.kafka.common.serialization.Serializer;

public class StreamMessageSerializer implements Serializer<StreamMessage> {

    @Override
    public byte[] serialize(String topic, StreamMessage data) {
        return data.getMessageData().toString().getBytes();
    }
}
