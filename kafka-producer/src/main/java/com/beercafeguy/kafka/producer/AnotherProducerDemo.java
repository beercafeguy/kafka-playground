package com.beercafeguy.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class AnotherProducerDemo {

    public static void main(String[] args) {

        final String topicName="azure_case_data";
        Properties properties=new Properties();
        properties.setProperty("bootstrap.servers","kafka1:9092,kafka2:9092,kafka3:9092");
        properties.setProperty("key.serializer",StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //acknowledgement from brokers
        properties.setProperty("acks","1"); //acks only from leader
        properties.setProperty("retries","3");

        Producer<String,String> producer=new KafkaProducer<String, String>(properties);
        int i=301;
        while(i<=400){
            ProducerRecord<String,String> record=new ProducerRecord<String, String>(topicName,String.valueOf(i%2),"Message no:"+i);
            producer.send(record);
            producer.flush();//message will be sent on flush
            System.out.println("Message sent with key:"+String.valueOf(i%3)+" and value Message no:"+i);
            i++;
        }

        producer.close();

    }
}
