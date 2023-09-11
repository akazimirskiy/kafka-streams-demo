package ru.kazimir.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import ru.kazimir.kafka.message.ObjectDeserializer;
import ru.kazimir.kafka.message.ObjectSerializer;

import java.util.Collections;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaConfigurator {

    private static final Logger log = LoggerFactory.getLogger(KafkaConfigurator.class);
    private static KafkaContainer kafkaContainer;
    private static Properties kafkaProps;
    public static Properties getKafkaProps() {
        if (Objects.isNull(kafkaProps)) {
            kafkaProps = new Properties();
            kafkaProps.put("key.serializer", StringSerializer.class);
            kafkaProps.put("key.deserializer", StringDeserializer.class);
            kafkaProps.put("value.serializer", ObjectSerializer.class);
            kafkaProps.put("value.deserializer", ObjectDeserializer.class);
            kafkaProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-demo");
            kafkaProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
            kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");}
        return kafkaProps;
    }

    public void init() throws ExecutionException, InterruptedException {
        log.info("Starting kafka container");
        kafkaContainer =  new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"))
                .withExposedPorts(9092, 9093);
        kafkaContainer.start();
        log.info("Kafka container has been started");
        try (Admin admin = Admin.create(getKafkaProps())) {
            int partitions = 1;
            short replicationFactor = 1;
            NewTopic newTopic = new NewTopic(Constants.STREAMING_TOPIC_NAME, partitions, replicationFactor);

            CreateTopicsResult result = admin.createTopics(
                    Collections.singleton(newTopic)
            );
            KafkaFuture<Void> future = result.values().get(Constants.STREAMING_TOPIC_NAME);
            future.get();
            log.info("Topic " + Constants.STREAMING_TOPIC_NAME  + " has been started");
        }
    }
    public void tearDown() {
        log.info("Stopping kafka container");
        kafkaContainer.stop();
        log.info("Kafka container has been stopped");
    }
}
