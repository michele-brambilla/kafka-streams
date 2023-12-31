package io.confluent.developer;

import io.confluent.developer.serde.JsonNodeSerde;

import com.fasterxml.jackson.databind.JsonNode;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;

public class KafkaStreamsApplication {

    private static final Logger logger = LoggerFactory.getLogger(KafkaStreamsApplication.class);

    static void runKafkaStreams(final KafkaStreams streams) {
        final CountDownLatch latch = new CountDownLatch(1);
        streams.setStateListener((newState, oldState) -> {
            if (oldState == KafkaStreams.State.RUNNING && newState != KafkaStreams.State.RUNNING) {
                latch.countDown();
            }
        });

        streams.start();

        try {
            latch.await();
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }

        logger.info("Streams Closed");
    }
    static Topology buildTopology(String inputTopic, String outputTopic) {
        Serde<String> stringSerde = Serdes.String();
        Serde<JsonNode> jsonSerde = new JsonNodeSerde();

        StreamsBuilder builder = new StreamsBuilder();

        builder
            .stream(inputTopic, Consumed.with(stringSerde, jsonSerde))
            .peek((k,v) -> logger.info("Observed event: {}", v))
            // .mapValues(s -> s.get("Events"))
            // .peek((k,v) -> logger.info("Transformed event: {}", v))
            .to((key, completedOrder, recordContext) -> recordContext.topic() + ".split", Produced.with(stringSerde, jsonSerde));

        return builder.build();
    }
    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            throw new IllegalArgumentException("This program takes one argument: the path to a configuration file.");
        }

        Properties props = new Properties();
        try (InputStream inputStream = new FileInputStream(args[0])) {
            props.load(inputStream);
        }

        final String inputTopic = props.getProperty("input.topic.name");
        final String outputTopic = props.getProperty("output.topic.name");

        try (Util utility = new Util()) {

            utility.createTopics(
                    props,
                    Arrays.asList(
                            new NewTopic(inputTopic, Optional.empty(), Optional.empty()),
                            new NewTopic(inputTopic + ".split", Optional.empty(), Optional.empty())));
            // try {
                KafkaStreams kafkaStreams = new KafkaStreams(buildTopology(inputTopic, outputTopic),props);

                Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

                logger.info("Kafka Streams 101 App Started");
                runKafkaStreams(kafkaStreams);

            // }
        }
    }
}
