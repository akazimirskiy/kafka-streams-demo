package ru.kazimir.kafka.message;

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class MessageGenerator extends Thread {

    private final Logger log;

    private final MessageSender messageSender;
    private boolean doStop;
    @Getter
    private final String generatorName;

    public MessageGenerator(String generatorName, MessageSender sender) {
        log = LoggerFactory.getLogger(generatorName);
        this.messageSender = sender;
        this.generatorName = generatorName;
        this.setName(generatorName);
    }

    @Override
    public void run() {
        log.info(getGeneratorName() + " has been started");
        while(!doStop) {
            try {
                messageSender.send(generateMessage());
                Thread.sleep((new Random()).nextLong(1000)+1000);
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
        log.info(getGeneratorName() + " has been stopped");
    }

    public void doStop() {
        doStop = true;
    }

    private StreamMessage generateMessage() {
        return () -> new MessageData(
                getGeneratorName(),
                Arrays.asList(MessageType.values()).get((new Random()).nextInt((MessageType.values().length))),
                (new Random()).nextFloat(10f));
    }
}
