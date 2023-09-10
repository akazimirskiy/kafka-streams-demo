package ru.kazimir.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ser.std.ClassSerializer;
import ru.kazimir.kafka.message.MessageData;
import ru.kazimir.kafka.message.StreamMessage;
import ru.kazimir.kafka.message.StreamMessageDeserializer;
import ru.kazimir.kafka.message.StreamMessageSerializer;

import java.time.Duration;
import java.util.List;
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
        Serde<String> stringSerde = Serdes.String();
        Serde<ValueAggregator> aggregatorSerde = Serdes.serdeFrom(Serializer)
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

        //Create a window of 5 seconds
        TimeWindows tumblingWindow = TimeWindows.of(Duration.ofSeconds(5)).grace(Duration.ZERO);
        Initializer<ValueAggregator> valueAggregatorInitializer = ValueAggregator::new;
        Aggregator<String, StreamMessage, ValueAggregator> valueAdder =
                (key, value, aggregate) -> aggregate.add(value.getMessageData().getBusinessValue().floatValue()); //TODO put function supplier here

        KTable<Windowed<String>, ValueAggregator> messageSummary = messageStream.groupBy(
                (key, value) -> value.getMessageData().getMessageType().toString(), Grouped.with(stringSerde, streamMessageSerdes))
                .windowedBy(tumblingWindow).aggregate(valueAggregatorInitializer, valueAdder, Materialized.<String, ValueAggregator,
                                WindowStore<Bytes, byte[]>>as(
                                "time-windowed-aggregate-store")
                        .withValueSerde(aggregatorSerde));
        )

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
