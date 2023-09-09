package ru.kazimir.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import ru.kazimir.kafka.message.StreamMessageDeserializer;
import ru.kazimir.kafka.message.StreamMessageSerializer;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

public class KafkaConfigurator {

    Logger log = Logger.getLogger(this.getClass().getSimpleName());
    private static KafkaContainer kafkaContainer;
    public static Properties getKafkaProps() {
        Properties kafkaProps = new Properties();
        kafkaProps.put("key.serializer", StringSerializer.class);
        kafkaProps.put("key.deserializer", StringDeserializer.class);
        kafkaProps.put("value.serializer", StreamMessageSerializer.class);
        kafkaProps.put("value.deserializer", StreamMessageDeserializer.class);
        kafkaProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-demo");
        kafkaProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        kafkaProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        kafkaProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        kafkaProps.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        kafkaProps.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
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
