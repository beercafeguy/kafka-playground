package com.beercafeguy.kafka.producer;

public class CsvKafkaProducerMain {

    public static void main(String[] args) {

        Thread producerThread=new CsvKafkaProducerThread("azure_employee_master_topic");
        producerThread.start();
    }
}
