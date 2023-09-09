package ru.kazimir.kafka.message;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;

@Data
@RequiredArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class MessageData {
    private final String generatorName;
    private final MessageType messageType;
    private final Float businessValue;
}
