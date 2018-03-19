package com.beercafeguy.kafka.producer.avro;

import com.beercafeguy.CustomerV1;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class AvroProducerDemoV1 {
    public static void main(String[] args) {
        Properties properties=new  Properties();
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
        properties.setProperty("acks","1");
        properties.setProperty("retries","10");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url","http://127.0.0.1:8081");
        KafkaProducer<String,CustomerV1> kafkaProducer=new KafkaProducer<String, CustomerV1>(properties);
        String topic="customer-avro";

        CustomerV1 customer=new CustomerV1().newBuilder()
                .setFirstName("John")
                .setLastName("Mant")
                .setAge(54)
                .setHeight(123f)
                .setWeight(123)
                .setAutoEmailTurnedOn(false)
                .build();
        ProducerRecord<String,CustomerV1> producerRecord=new ProducerRecord<String, CustomerV1>(topic,customer);

        kafkaProducer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(null==e){
                    System.out.println("Success");
                    System.out.println(recordMetadata.toString());
                }
            }
        });

        kafkaProducer.flush();
    }
}
