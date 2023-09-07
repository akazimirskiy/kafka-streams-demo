package ru.kazimir.kafka.message;

import lombok.Getter;
import org.apache.kafka.clients.producer.ProducerRecord;
import ru.kazimir.kafka.Constants;

import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

public class MessageGenerator extends Thread {

    Logger log = Logger.getLogger(this.getClass().getSimpleName());

    private MessageSender messageSender;
    private long timeoutMS = 1000;
    private boolean doStop;
    @Getter
    private String generatorName;

    public MessageGenerator(String generatorName, MessageSender sender) {
        this.messageSender = sender;
        this.generatorName = generatorName;
    }

    @Override
    public void run() {
        log.info("MessageGenerator has been started");
        while(!doStop) {
            ProducerRecord<String, StreamMessage> message = new ProducerRecord<>(Constants.STREAMING_TOPIC_NAME, generateMessage());
            try {
                messageSender.send(generateMessage());
                log.info("Message sent " + message);
                Thread.sleep(timeoutMS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
        log.info("MessageGenerator has been stopped");
    }

    public void doStop() {
        doStop = true;
    }

    private StreamMessage generateMessage() {
        return new StreamMessage() {
            @Override
            public MessageData getMessageData() {
                return new MessageData(getGeneratorName());
            }
        };
    }
}
