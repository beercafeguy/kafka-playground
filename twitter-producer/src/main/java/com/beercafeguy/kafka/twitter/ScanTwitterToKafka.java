package com.beercafeguy.kafka.twitter;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class ScanTwitterToKafka {

	private static final String topic = "holi-tweets";

	public static void main(String[] args) throws IOException {
		Properties props=new ScanTwitterToKafka().getProperties("src/main/resources/producer.properties");
		String consumerKey=props.getProperty("consumer_key");
		String consumerSecret=props.getProperty("consumer_secret");
		String token=props.getProperty("access_token");
		String tokenSecret=props.getProperty("access_token_secret");
		System.out.println("Consumer key:"+consumerKey);
		ScanTwitterToKafka.run(consumerKey, consumerSecret,token, tokenSecret);
	}

	private static void run(String consumerKey, String consumerSecret, String token, String secret) {
		Properties properties = new Properties();
		properties.put("metadata.broker.list", "localhost:9092");
		properties.put("serializer.class", "kafka.serializer.StringEncoder");
		properties.put("client.id", "beercafeguy");
		ProducerConfig producerConfig = new ProducerConfig(properties);

		kafka.javaapi.producer.Producer<String, String> producer = new kafka.javaapi.producer.Producer<String, String>(
				producerConfig);
		BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);
		StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
		endpoint.trackTerms(Lists.newArrayList("twitterapi", "#HoliHai"));
		Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);
		Client client = new ClientBuilder().hosts(Constants.STREAM_HOST).endpoint(endpoint).authentication(auth)
				.processor(new StringDelimitedProcessor(queue)).build();
		client.connect();
		// Do whatever needs to be done with messages
		for (int msgRead = 0; msgRead < 1000; msgRead++) {
			KeyedMessage<String, String> message = null;
			try {
				message = new KeyedMessage<String, String>(topic, queue.take());
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			producer.send(message);
		}
		producer.close();
		client.stop();

	}
	private Properties getProperties(String filename) throws IOException {
		InputStream is = new FileInputStream(filename);
		Properties props=new Properties();
		props.load(is);
		return props;
	}

}
