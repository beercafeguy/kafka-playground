package com.beercafeguy.kafka.producer;

public class CallbackProducerMain {
    public static void main(String[] args) {
        ProducerWithCallbackDemo producerThread = new ProducerWithCallbackDemo("azure_case_data_int");
        producerThread.start();
    }
}
