package com.github.beercafeguy.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Properties;

public class UserEventEnricher {
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(UserEventEnricher.class);
        logger.info("Enriching User Events ....");
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "UserEventEnricher");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KStreamBuilder builder = new KStreamBuilder();
        GlobalKTable<String,String> usersGlobalTable=builder.globalTable("users-table");

        KStream<String,String> userPurchases=builder.stream("user-purchases");
        KStream<String,String> enrichedPurchaseData = userPurchases.join(
                usersGlobalTable,
                (key,value) -> key,
                (userPurchase,userInfo) -> "Purchase = "+userPurchase+", User Info = [ "+userInfo+" ]"
        );

        enrichedPurchaseData.to("enriched-purchase-data-inner");

         KStream<String,String> userPurchaseEnrichedLeftJoin=userPurchases.
                 leftJoin(usersGlobalTable,
                         (key,value) -> key,
                         (userPurchase,userInfo) -> {
                            if(userInfo!=null){
                                return "Purchase = "+userPurchase+", User Info = [ "+userInfo+" ]";
                            }else{
                                return "Purchase = "+userPurchase+", User Info = [ null ]";
                            }
                            }
                         );

        userPurchaseEnrichedLeftJoin.to(Serdes.String(),Serdes.String(),"enriched-purchase-data-leftjoin");

        KafkaStreams streams=new KafkaStreams(builder,config);
        streams.cleanUp();
        streams.start();

        logger.info(streams.toString());
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
