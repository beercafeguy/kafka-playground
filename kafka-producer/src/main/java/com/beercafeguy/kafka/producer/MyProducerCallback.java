package com.beercafeguy.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class MyProducerCallback implements Callback {

	public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
		if(null!=exception) {
			exception.printStackTrace();
		}

	}

}
