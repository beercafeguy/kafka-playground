package com.github.beercafeguy.kafka.streams;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class BankTransactionProducerApp {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(BankTransactionProducerApp.class);
        logger.info("Starting transactions ....");


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
        String topicName="bank-transactions";
        int i=1;
        while(true){
            logger.info("Producing Batch :"+i);
            try{
                producer.send(getRandomTransaction("Hem",topicName));
                Thread.sleep(100);
                producer.send(getRandomTransaction("Aman",topicName));
                Thread.sleep(100);
                producer.send(getRandomTransaction("Ajay",topicName));
                Thread.sleep(100);
                i++;
            }catch (InterruptedException ex){
                logger.error(ex.getMessage());
                break;
            }
        }
    }

    public static ProducerRecord<String,String> getRandomTransaction(String name,String topic){
        ObjectNode transaction=JsonNodeFactory.instance.objectNode();
        Integer amount=ThreadLocalRandom.current().nextInt(0,100);
        Instant now=Instant.now();
        transaction.put("name",name);
        transaction.put("amount",amount);
        transaction.put("time",now.toString());
        return new ProducerRecord<String,String>(topic,name,transaction.toString());
    }
}
