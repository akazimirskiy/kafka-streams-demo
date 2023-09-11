package ru.kazimir.kafka.message;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class StreamMessageImpl implements StreamMessage {
    private MessageData messageData;
}
