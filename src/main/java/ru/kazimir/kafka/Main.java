package ru.kazimir.kafka;

import ru.kazimir.kafka.message.MessageGenerator;
import ru.kazimir.kafka.message.MessageSender;

import java.util.List;
import java.util.concurrent.ExecutionException;

public class Main {

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        Configurator config = new Configurator();
        config.init();

        MessageSender sender = new MessageSender();
        List<MessageGenerator> generators = List.of(
                new MessageGenerator("Generator 1", sender),
                new MessageGenerator("Generator 2", sender),
                new MessageGenerator("Generator 3", sender)
                );
        generators.forEach(MessageGenerator::start);
        Thread.sleep(10000);
        generators.forEach(MessageGenerator::doStop);
        generators.forEach(messageGenerator -> {
            try {
                messageGenerator.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        config.tearDown();
    }
}
