package com.beercafeguy.kafka.producer;

public class CsvToJsonProducerMain {
    public static void main(String[] args) {
        CsvToJsonProducerThread csvToJsonProducerThread=new CsvToJsonProducerThread("employee_json_topic");
        csvToJsonProducerThread.start();
    }

}
