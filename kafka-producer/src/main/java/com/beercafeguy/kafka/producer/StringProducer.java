package com.beercafeguy.kafka.producer;

import java.time.LocalDateTime;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class StringProducer {

	private static final Properties props=new Properties();
	private static final String topic="test1";
	
	public static void main(String[] args) {		
		props.put("bootstrap.servers", "no1010042056227.corp.adobe.com:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		produce(topic, props);
		syncProduce(topic,props);
		asyncProduce(topic,props);
	}
	
	
	//fire and forget approach
	private static void produce(String topic,Properties kafkaProps) {
		System.out.println("Start time:"+LocalDateTime.now());
		Producer<String,String> producer=new KafkaProducer<String,String>(kafkaProps);
		int i=0;
		while(i<101) {
			ProducerRecord<String,String> record=new ProducerRecord<String,String>(topic,Integer.toString(i),Integer.toString(i+1));
			try {
			producer.send(record);
			}catch(Exception e) {
				e.printStackTrace();
			}
			System.out.println("Number Sent:"+i);
			i++;
		}
		
		System.out.println("End time:"+LocalDateTime.now());
	}
	
	
	//send and wait till completion
	private static void syncProduce(String topic,Properties kafkaProps) {
		System.out.println("Start time for sync:"+LocalDateTime.now());
		Producer<String,String> producer=new KafkaProducer<String,String>(kafkaProps);
		int i=0;
		while(i<101) {
			ProducerRecord<String,String> record=new ProducerRecord<String,String>(topic,Integer.toString(i),Integer.toString(i+1));
			try {
			producer.send(record).get();// get() call is the only difference between sync and async produce
			}catch(Exception e) {
				e.printStackTrace();
			}
			System.out.println("Number Sent using Sync:"+i);
			i++;
		}
		System.out.println("End time for sync:"+LocalDateTime.now());
	}
	
	//send async way
		private static void asyncProduce(String topic,Properties kafkaProps) {
			System.out.println("Start time for async:"+LocalDateTime.now());
			Producer<String,String> producer=new KafkaProducer<String,String>(kafkaProps);
			int i=0;
			while(i<101) {
				ProducerRecord<String,String> record=new ProducerRecord<String,String>(topic,Integer.toString(i),Integer.toString(i+1));
				try {
				producer.send(record,new MyProducerCallback());//adding callback object for async operation
				}catch(Exception e) {
					e.printStackTrace();
				}
				System.out.println("Number Sent using async:"+i);
				i++;
			}
			System.out.println("End time for async:"+LocalDateTime.now());
		}

}
