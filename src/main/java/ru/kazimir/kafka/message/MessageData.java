package ru.kazimir.kafka.message;

import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@RequiredArgsConstructor
public class MessageData {
    private final String generatorName;
    public MessageData(byte[] byteArray) {
        this.generatorName = ""; //TODO
    }
}
