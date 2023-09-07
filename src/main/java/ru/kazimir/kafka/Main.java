package ru.kazimir.kafka;

import ru.kazimir.kafka.message.MessageGenerator;
import ru.kazimir.kafka.message.MessageSender;

import java.util.concurrent.ExecutionException;

public class Main {

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        Configurator config = new Configurator();
        config.init();

        MessageGenerator messageGenerator = new MessageGenerator("Generator 1", new MessageSender());
        messageGenerator.start();
        Thread.sleep(10000);
        messageGenerator.doStop();
        config.tearDown();
    }
}
