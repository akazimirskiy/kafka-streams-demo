package ru.kazimir.kafka;

import lombok.Data;
import ru.kazimir.kafka.message.MessageType;

@Data
public class ValueAggregator {

    MessageType type;
    float totalValue;

    public ValueAggregator add(float value) {
        totalValue += value;
        return this;
    }
}
