package com.github.beercafeguy.kafka.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class BankBalanceStreamingApp {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(BankBalanceStreamingApp.class);
        logger.info("Calculating Bank balance....");
        //System.out.println("Hello Kafka Streams");
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "BankBalanceCalculator");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        //config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        //config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,StreamsConfig.EXACTLY_ONCE); // guaranty for exactly once

        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,"0");

        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //creating Serde
        final Serializer<JsonNode> jsonSerializer=new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer=new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer,jsonDeserializer);

        KStreamBuilder builder = new KStreamBuilder();
        KStream<String,JsonNode> transactions=builder.stream(
                Serdes.String(),
                jsonSerde,
                "bank-transactions"
        );


        ObjectNode initialBalance=JsonNodeFactory.instance.objectNode();
        initialBalance.put("count",0);
        initialBalance.put("balance",0);
        initialBalance.put("time",Instant.ofEpochMilli(0L).toString());

        KTable<String,JsonNode> bankBalance=transactions.
                groupByKey(Serdes.String(),jsonSerde).
                aggregate(
                () -> initialBalance,
                (key,transaction,balance) -> getBalance(transaction,balance),
                jsonSerde,
                "bank-balance-agg"
        );

        bankBalance.to(Serdes.String(),jsonSerde,"bank-balance");
        KafkaStreams streams=new KafkaStreams(builder,config);
        streams.cleanUp();
        streams.start();

        logger.info(streams.toString());
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public static JsonNode getBalance(JsonNode transaction,JsonNode balance){
        ObjectNode newBalance=JsonNodeFactory.instance.objectNode();
        newBalance.put("count",balance.get("count").asInt()+1);
        newBalance.put("balance",balance.get("balance").asInt()+transaction.get("amount").asInt());

        Long balanceEpoch=Instant.parse(balance.get("time").asText()).toEpochMilli();
        Long transactionEpoch=Instant.parse(transaction.get("time").asText()).toEpochMilli();
        Instant newBalanceInstant=Instant.ofEpochMilli(Math.max(balanceEpoch,transactionEpoch));
        newBalance.put("time",newBalanceInstant.toString());
        return newBalance;
    }

    private static int getAmount(String transaction) throws IOException {
        ObjectMapper mapper=new ObjectMapper();
        JsonNode node=mapper.readTree(transaction);
        return node.get("amount").asInt();
    }
}
