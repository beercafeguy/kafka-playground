package com.github.beercafeguy.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class FavColorApp {
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(FavColorApp.class);
        logger.info("Favourite Color App");
        //System.out.println("Hello Kafka Streams");
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-color-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,StreamsConfig.EXACTLY_ONCE); // guaranty for exactly once

        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,"0");

        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, String> userColorStream = builder.stream(
                        Serdes.String(),
                        Serdes.String(),
                        "user-color-uses");

        KStream<String,String> sourceStream=userColorStream.peek((key,value)->logger.info("Key -> "+key+" | Value -> "+value));

        String[] colorArray={"RED","BLUE","GREEN"};
        List<String> colors= Arrays.asList(colorArray);
        KStream<String,String> nameColorStream=sourceStream
                .filter(
                        (key,value) ->
                                value.contains(",")
                                && value.split(" ").length == 2
                                && colors.contains(value.split(" ")[1]) )
                .selectKey((key,value) -> value.split(",")[0].toLowerCase()) //will trigger a repar
                .mapValues(value -> value.split(" ")[1].toLowerCase());
        nameColorStream.to(Serdes.String(),Serdes.String(),"color-count-compact-tmp");

        KTable<String, String> colorCountTable = builder.table(
                Serdes.String(),
                Serdes.String(),
                "color-count-compact-tmp");

        KTable<String,Long> favColor=colorCountTable
                .groupBy((user,color) -> new KeyValue<>(color,color))
                .count("CountByColor");
        favColor.to(Serdes.String(),Serdes.Long(),"fav-color-output");

        KafkaStreams streams=new KafkaStreams(builder,config);
        streams.cleanUp();
        streams.start();

        logger.info(streams.toString());
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
