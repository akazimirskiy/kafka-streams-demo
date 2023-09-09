package ru.kazimir.kafka.message;

import lombok.Getter;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

public class MessageGenerator extends Thread {

    Logger log = Logger.getLogger(this.getClass().getSimpleName());

    private final MessageSender messageSender;
    private boolean doStop;
    @Getter
    private final String generatorName;

    public MessageGenerator(String generatorName, MessageSender sender) {
        this.messageSender = sender;
        this.generatorName = generatorName;
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
