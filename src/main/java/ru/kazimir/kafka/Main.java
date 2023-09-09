package ru.kazimir.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.kazimir.kafka.message.MessageGenerator;
import ru.kazimir.kafka.message.MessageSender;

import java.util.List;
import java.util.concurrent.ExecutionException;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        KafkaConfigurator kafkaConfig = new KafkaConfigurator();
        kafkaConfig.init();
        StreamAnalyzer streamAnalyzer = new StreamAnalyzer();
        MessageSender sender = new MessageSender();
        List<MessageGenerator> generators = List.of(
                new MessageGenerator("Generator 1", sender),
                new MessageGenerator("Generator 2", sender),
                new MessageGenerator("Generator 3", sender)
        );
        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                log.info("Stop signal detected. Shutting down");
                streamAnalyzer.doStop();
                generators.forEach(MessageGenerator::doStop);
                generators.forEach(messageGenerator -> {
                    try {
                        messageGenerator.join();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
                kafkaConfig.tearDown();
                log.info("Shutdown completed");
            }
        });

        generators.forEach(MessageGenerator::start);
        streamAnalyzer.start();
    }
}
