package com.github.beercafeguy.kafka.producer;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;

public class UserEventProducer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Logger logger = LoggerFactory.getLogger(UserEventProducer.class);
        logger.info("Starting user events ....");


        Properties config = new Properties();
        //config.put(StreamsConfig.APPLICATION_ID_CONFIG, "bank-transactions-producer");

        config.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        config.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        config.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        config.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        config.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");

        Producer<String,String> producer=new KafkaProducer<String, String>(config);
        String userTableTopicName="users-table";
        String userPurchasesTopicName="user-purchases";

        logger.info("Example 1: -> New User");
        producer.send(getProducerRecord("hem","{name = hem, lastN = chandra, email = hem.chandra@gmail.com}",userTableTopicName)).get();
        producer.send(getProducerRecord("hem","{item = iPhoneX, price = 100}",userPurchasesTopicName)).get();

        Thread.sleep(10000);

        logger.info("Example 1: -> Non Existing user");
        producer.send(getProducerRecord("bob","{item = miA2, price = 50}",userPurchasesTopicName)).get();
        Thread.sleep(10000);

        logger.info("Example 1: -> Update to User");
        producer.send(getProducerRecord("hem","{name = hem, lastN = Thuwal, email = hem.chandra@yahoo.com}",userTableTopicName)).get();
        producer.send(getProducerRecord("hem","{item = Apple, price = 2}",userPurchasesTopicName)).get();
        Thread.sleep(10000);

        logger.info("Example 1: -> Create User After Purchase");
        producer.send(getProducerRecord("sonu","{item = Musli, price = 2}",userPurchasesTopicName)).get();
        producer.send(getProducerRecord("sonu","{name = Sonu, lastN = Dafauti, email = sonu.dafauti@yahoo.com}",userTableTopicName)).get();
        producer.send(getProducerRecord("sonu","{item = Book, price = 10}",userPurchasesTopicName)).get();
        producer.send(getProducerRecord("sonu",null,userTableTopicName)).get();
        Thread.sleep(10000);

        logger.info("Example 1: -> Create User but delete before purchase comes");

        producer.send(getProducerRecord("prashant","{name = prashant, lastN = gupta, email = prashant.gupta@yahoo.com}",userTableTopicName)).get();
        producer.send(getProducerRecord("prashant",null,userTableTopicName)).get();
        producer.send(getProducerRecord("prashant","{item = Gutkha, price = 2}",userPurchasesTopicName)).get();
        Thread.sleep(10000);


        logger.info("End Of Demo");
        producer.close();

        Runtime.getRuntime().addShutdownHook(new Thread(producer::close));
    }

    public static ProducerRecord<String,String> getProducerRecord(String key,String value,String topic){
        return new ProducerRecord<String,String>(topic,key,value);
    }
}
