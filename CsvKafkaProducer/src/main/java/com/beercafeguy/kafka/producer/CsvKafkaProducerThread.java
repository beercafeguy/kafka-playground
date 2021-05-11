package com.beercafeguy.kafka.producer;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class CsvKafkaProducerThread extends Thread{
    private final KafkaProducer<Integer, String> kafkaProducer;
    private final String topicName;
    private final Boolean isAsync;

    private static Properties getProps() {
        Properties properties = null;
        try {
            return PropertyFactory.getProperties("src/main/resources/kafka.server.properties");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }

    public CsvKafkaProducerThread(String topicName) {
        Properties kafkaServerProps = getProps();
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServerProps.getProperty("bootstrep.servers"));
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "CallbackProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProducer = new KafkaProducer<>(props);
        this.topicName = topicName;
        this.isAsync = Integer.parseInt(kafkaServerProps.getProperty("kafka.consumer.sync")) == 1;
    }

    @Override
    public void run() {
        try (Reader in = new FileReader("src/main/resources/employee.csv")) {
            Iterable<CSVRecord> csvRecords = CSVFormat.DEFAULT
                    .withHeader("id", "name","salary").withSkipHeaderRecord().parse(in);
            for (CSVRecord csvRecord : csvRecords) {

                Integer key=Integer.parseInt(csvRecord.get("id"));
                String message="id -> "+csvRecord.get("id")+": name -> "+csvRecord.get("name")+": salary -> "+csvRecord.get("salary");

                long startTime = System.currentTimeMillis();

                if (isAsync) {
                    System.out.println("Producer type is async");
                    ProducerRecord<Integer, String> record = new ProducerRecord<Integer, String>(topicName, key, message);
                    try {
                        kafkaProducer.send(record).get();
                        System.out.println("Sent message: (" + key + ", " + message + ")");
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }
                } else {
                    System.out.println("Producer type is sync");
                    ProducerRecord<Integer, String> record = new ProducerRecord<Integer, String>(topicName, key, message);
                    kafkaProducer.send(record, new MyCallback(key, message, startTime));
                    System.out.println("Sent message: (" + key + ", " + message + ")");

                }

            }
        }catch(Exception ex){
            ex.printStackTrace();
        }
    }
}
