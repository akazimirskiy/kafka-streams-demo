package ru.kazimir.kafka;

import lombok.Data;

@Data
public class ValueAggregator {

    float totalValue;

    public ValueAggregator add(float value) {
        totalValue += value;
        return this;
    }
}
