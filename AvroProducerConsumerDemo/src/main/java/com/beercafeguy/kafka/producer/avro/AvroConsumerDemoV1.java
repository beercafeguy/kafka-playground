package com.beercafeguy.kafka.producer.avro;

import com.beercafeguy.CustomerV1;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

public class AvroConsumerDemoV1 {
    public static void main(String[] args) {
        Properties properties=new  Properties();
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
        properties.setProperty("group.id","beercafe-customer-group");
        properties.setProperty("enable.auto.commit","false");
        properties.setProperty("auto.offset.reset","earliest");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", KafkaAvroDeserializer.class.getName());
        properties.setProperty("schema.registry.url","http://127.0.0.1:8081");
        properties.setProperty("specific.avro.reader","true");

        KafkaConsumer<String,CustomerV1> kafkaConsumer=new KafkaConsumer<String, CustomerV1>(properties);
        String topic="customer-avro";

        kafkaConsumer.subscribe(Collections.singleton(topic));
        System.out.println("Starting consume process..");
        while(true){

            ConsumerRecords<String,CustomerV1> records=kafkaConsumer.poll(500);
            for (ConsumerRecord<String,CustomerV1> record:records) {
                CustomerV1 customer=record.value();
                System.out.println("Customer:"+customer);
                if(customer.getAge()>50){
                    System.out.println("Old customer logged in :)");
                }else{
                    System.out.println("Young customer logged in.");
                }
            }
        }
    }
}
