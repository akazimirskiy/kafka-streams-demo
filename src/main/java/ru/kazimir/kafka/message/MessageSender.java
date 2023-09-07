package ru.kazimir.kafka.message;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import ru.kazimir.kafka.Configurator;
import ru.kazimir.kafka.Constants;

import java.util.concurrent.ExecutionException;

public class MessageSender {
    private Producer<String, StreamMessage> messageProducer;
    public MessageSender() {
        messageProducer = new KafkaProducer<String, StreamMessage>(Configurator.getKafkaProps());
    }
    public void send(StreamMessage message) throws ExecutionException, InterruptedException {
        ProducerRecord<String, StreamMessage> record = new ProducerRecord<>(Constants.STREAMING_TOPIC_NAME, message);
        messageProducer.send(record).get();
    }
}
