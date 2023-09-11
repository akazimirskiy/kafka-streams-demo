package ru.kazimir.kafka.message;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.kazimir.kafka.KafkaConfigurator;
import ru.kazimir.kafka.Constants;

import java.util.concurrent.ExecutionException;

public class MessageSender {
    private static final Logger log = LoggerFactory.getLogger(MessageSender.class);
    private final Producer<String, StreamMessageImpl> messageProducer;
    public MessageSender() {
        messageProducer = new KafkaProducer<>(KafkaConfigurator.getKafkaProps());
    }
    public void send(StreamMessageImpl message) throws ExecutionException, InterruptedException {
        ProducerRecord<String, StreamMessageImpl> record = new ProducerRecord<>(Constants.STREAMING_TOPIC_NAME, message);
        messageProducer.send(record).get();
        //log.info("Message sent " + message);
    }
}
