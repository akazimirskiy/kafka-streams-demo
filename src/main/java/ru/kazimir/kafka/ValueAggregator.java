package ru.kazimir.kafka;

import lombok.Data;
import ru.kazimir.kafka.message.MessageType;

@Data
public class ValueAggregator {

//    private final Map<MessageType, Float> aggregator = new HashMap<>(MessageType.values().length);
    private final MessageType type = MessageType.TYPE1;
    float totalValue;

    public ValueAggregator() {
//        Arrays.stream(MessageType.values()).forEach((k)->aggregator.put(k, Float.valueOf(0f)));
    }

    public ValueAggregator add(float value) {
//        aggregator.put(messageData.getMessageType(), aggregator.get(messageData.getMessageType()).floatValue() + messageData.getBusinessValue());
        totalValue += value;
        return this;
    }
}
