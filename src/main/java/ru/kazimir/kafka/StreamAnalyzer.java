package ru.kazimir.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.kazimir.kafka.message.MessageData;
import ru.kazimir.kafka.message.StreamMessage;
import ru.kazimir.kafka.message.StreamMessageDeserializer;
import ru.kazimir.kafka.message.StreamMessageSerializer;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class StreamAnalyzer {
    Logger log = LoggerFactory.getLogger(this.getClass().getSimpleName());
    ObjectMapper objectMapper = new ObjectMapper();
    KafkaStreams streams;
    CountDownLatch latch;

    public void start() {
        Serde<StreamMessage> streamMessageSerdes = Serdes.serdeFrom(
                new StreamMessageSerializer(),
                new StreamMessageDeserializer());
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streaming-analytics-pipe");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfigurator.getKafkaProps().get(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, streamMessageSerdes.getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,KafkaConfigurator.getKafkaProps().get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
        //For immediate results during testing
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);

        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> objectStream = streamsBuilder.stream(
                Constants.STREAMING_TOPIC_NAME, Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, StreamMessage> messageStream = objectStream.mapValues(
                value -> {
                    try {
                        MessageData messageData = objectMapper.readValue(value, MessageData.class);
                        return () -> messageData;
                    } catch (JsonProcessingException e) {
                        log.info("Failed parsing message. Skipping object: {}", value);
                    }
                    return null;
                }
        );

        streamsBuilder.stream(Constants.STREAMING_TOPIC_NAME, Consumed.with(Serdes.String(), streamMessageSerdes));
        messageStream.peek(((key, value) -> log.info("Received message " + value.getMessageData())));

        final Topology topology = streamsBuilder.build();
        log.info(topology.describe().toString());

        //Setup Stream
        streams = new KafkaStreams(topology, props);
        streams.cleanUp();
        latch = new CountDownLatch(1);

        //Start the stream
        streams.start();
        //Await termination
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    public void doStop() {
        log.info("StreamAnalyzer is stopping..");
        streams.close();
        latch.countDown();
        log.info("StreamAnalyzer is stopped");
    }
}
