package ru.kazimir.kafka.message;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import ru.kazimir.kafka.KafkaConfigurator;
import ru.kazimir.kafka.Constants;

import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

public class MessageSender {
    Logger log = Logger.getLogger(this.getClass().getSimpleName());
    private final Producer<String, StreamMessage> messageProducer;
    public MessageSender() {
        messageProducer = new KafkaProducer<>(KafkaConfigurator.getKafkaProps());
    }
    public void send(StreamMessage message) throws ExecutionException, InterruptedException {
        ProducerRecord<String, StreamMessage> record = new ProducerRecord<>(Constants.STREAMING_TOPIC_NAME, message);
        messageProducer.send(record).get();
        log.info("Message sent " + message.getMessageData());
    }
}
