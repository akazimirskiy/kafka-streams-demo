package ru.kazimir.kafka.message;

import lombok.Getter;

import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

public class MessageGenerator extends Thread {

    Logger log = Logger.getLogger(this.getClass().getSimpleName());

    private final MessageSender messageSender;
    private long timeoutMS = 1000;
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
                Thread.sleep(timeoutMS);
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
        return () -> new MessageData(getGeneratorName());
    }
}
