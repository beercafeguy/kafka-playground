package com.github.beercafeguy.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

public class StreamsStarterApp {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(LoggingApp.class);
        logger.info("Hello Kafka Streams");
        //System.out.println("Hello Kafka Streams");
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KStreamBuilder builder = new KStreamBuilder();

        //1. Stream from Kafka
        //KStream<String, String> stringStream = builder.stream(
        //        Serdes.String(),
        //        Serdes.String(),
        //        "word-count-input1");

        KStream<String, String> wcInputStream = builder.stream("word-count-input1");

        //2. Operate
        KTable<String, Long> wordsCount = wcInputStream
                .mapValues(value -> value.toLowerCase()) //Map values to lower case
                .flatMapValues(value -> Arrays.asList(value.split(" "))) //split values for form stream of words
                .selectKey((key, value) -> value) //replace key with value
                .groupByKey() // Group by Key
                .count("Counts"); //Count

        wordsCount.to(Serdes.String(),Serdes.Long(),"word-count-output1");
        KafkaStreams streams=new KafkaStreams(builder,config);
        streams.start();

        //print topology
        System.out.println(streams.toString());

        // adding shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
