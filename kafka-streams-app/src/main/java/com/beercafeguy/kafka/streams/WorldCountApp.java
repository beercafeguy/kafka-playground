package com.beercafeguy.kafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Properties;

public class WorldCountApp {
    public static void main(String[] args) {
        Properties properties=new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG,"WC");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"kafka1:9092,kafka2:9092,kafka3:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());


        StreamsBuilder builder=new StreamsBuilder();
        KStream<String,String> textLines=builder.stream("TextLinesTopic");

        KTable<String,Long> wordCounts=textLines.flatMapValues(textLine -> Arrays.asList(textLine.toLowerCase().split("\\W+"))).
                groupBy((key,word) -> word).
                count(Materialized.<String,Long,KeyValueStore<Bytes,byte[]>>as("count-store"));

        wordCounts.toStream().to("WordsWithCountsTopic", Produced.with(Serdes.String(),Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);

        streams.start();
    }
}
