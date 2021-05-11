package com.beercafeguy.kafka.producer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class StringConsumerDemo {

    public static void main(String[] args) {

        final String topicName="azure_case_data";
        Properties properties=new Properties();
        properties.setProperty("bootstrap.servers","kafka1:9092,kafka2:9092,kafka3:9092");
        properties.setProperty("key.deserializer",StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        //acknowledgement from brokers
        properties.setProperty("group.id","fin_users_group"); //acks only from leader
        properties.setProperty("enable.auto.commit","true");
        properties.setProperty("auto.commit.interval.ms","1000");
        properties.setProperty("auto.offset.reset","earliest");

        Consumer<String,String> consumer=new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topicName,"azure_case_data_1"));//reading from two topics for testing purpose only

        while(true){
            ConsumerRecords<String,String> records=consumer.poll(100);
            for (ConsumerRecord<String,String> record:records) {
                System.out.println("Timestamp:"+record.timestamp()+", Key:"+record.key()+", Value:"+record.value()+", Partition:"+record.partition()+", Offset:"+record.offset());
            }
            //use only when enable.auto.commit=false
            //consumer.commitSync();
        }

    }
}
