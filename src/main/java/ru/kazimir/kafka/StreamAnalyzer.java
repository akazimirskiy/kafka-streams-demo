package ru.kazimir.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.kazimir.kafka.message.MessageData;
import ru.kazimir.kafka.message.ObjectDeserializer;
import ru.kazimir.kafka.message.ObjectSerializer;
import ru.kazimir.kafka.message.StreamMessageImpl;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class StreamAnalyzer {
    Logger log = LoggerFactory.getLogger(this.getClass().getSimpleName());
    ObjectMapper objectMapper = new ObjectMapper();
    KafkaStreams streams;
    CountDownLatch latch;

    public void start() {
        Serde<MessageData> messageDataSerde = Serdes.serdeFrom(
                new ObjectSerializer<>(),
                new ObjectDeserializer<>(MessageData.class));
        Deserializer<StreamMessageImpl> messageDataDeserializer = new ObjectDeserializer<>(StreamMessageImpl.class);//MessageDataDeserializer();
        Serde<String> keySerde = Serdes.String();
        Serde<ValueAggregator> aggregatorSerde = Serdes.serdeFrom(
                new ObjectSerializer<>(),
                new ObjectDeserializer<>(ValueAggregator.class));
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streaming-analytics-pipe");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfigurator.getKafkaProps().get(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,KafkaConfigurator.getKafkaProps().get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
        //For immediate results during testing
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);

        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> objectStream = streamsBuilder.stream(
                Constants.STREAMING_TOPIC_NAME, Consumed.with(Serdes.String(), Serdes.String()));
        //objectStream.peek((key, value) -> log.info("Received object key = " + key + ", value = " + value));

        KStream<String, MessageData> messageStream = objectStream.mapValues(
                inputJson -> {
                    try {
                        return messageDataDeserializer.deserialize(Constants.STREAMING_TOPIC_NAME, inputJson.getBytes()).getMessageData();
                    }
                    catch(Exception e) {
                        log.warn("ERROR : Cannot convert JSON {}. Error : {}", inputJson, e.getMessage());
                        return null;
                    }
                }
        );

        Initializer<ValueAggregator> valueAggregatorInitializer = ValueAggregator::new;
        Aggregator<String, MessageData, ValueAggregator> valueAdder =
                (key, value, aggregator) -> aggregator.add(value.getBusinessValue()); //TODO put function supplier here

        KTable<String, ValueAggregator> messageValueSummary = messageStream.groupBy(
                new KeyValueMapper<String, MessageData, String>() {
                    @Override
                    public String apply(String key, MessageData value) {
                        return key;
                    }
                }, Grouped.with(Serdes.String(), messageDataSerde)
        ).aggregate(valueAggregatorInitializer, valueAdder);

        messageValueSummary.toStream().peek((key, value)->log.info("Aggregated message key = " + key + ", value = " + value.getTotalValue()));

        //messageStream.peek((key, value) -> log.info("Received message key = " + key + ", value = " + value));


        //Create a window of 5 seconds
//        TimeWindows tumblingWindow = TimeWindows.of(Duration.ofSeconds(5)).grace(Duration.ZERO);
//
//        KTable<String, ValueAggregator> messageSummary =
//                messageStream.groupBy((key, value) -> value.getMessageType().toString(),
//                                Grouped.with(keySerde, messageDataSerde))
//                        .aggregate(valueAggregatorInitializer, valueAdder);
//        messageSummary.toStream().foreach((k, v)->log.info("Key = {}. Value = {}", k, v.totalValue));
//        KTable<Windowed<String>, ValueAggregator> messageSummary = messageStream.groupBy(
//                (key, value) -> value.getMessageType().toString(),
//                        Grouped.with(keySerde, messageDataSerde))
//                .windowedBy(tumblingWindow).aggregate(
//                        valueAggregatorInitializer,
//                        valueAdder,
//                        Materialized
//                                .<String, ValueAggregator, WindowStore<Bytes, byte[]>>as("time-windowed-aggregate-store")
//                                .withValueSerde(aggregatorSerde))
//                .suppress(
//                        Suppressed
//                                .untilWindowCloses(
//                                        Suppressed.BufferConfig
//                                                .unbounded()
//                                                .shutDownWhenFull()));
        //messageSummary.toStream().foreach((key, value)->log.info("{} = {}", value.getType(), value.getTotalValue()));

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
