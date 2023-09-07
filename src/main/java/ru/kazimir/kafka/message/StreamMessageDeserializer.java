package ru.kazimir.kafka.message;

import org.apache.kafka.common.serialization.Deserializer;

public class StreamMessageDeserializer implements Deserializer<StreamMessage> {

    @Override
    public StreamMessage deserialize(String topic, byte[] data) {
        return new StreamMessage() {
            @Override
            public MessageData getMessageData() {
                return new MessageData(data);
            }
        };
    }
}
