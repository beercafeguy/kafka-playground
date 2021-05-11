package com.github.beercafeguy.kafka.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BankTransactionProducerTest {

    @Test
    public void checkTransaction(){
       ProducerRecord<String,String> record=BankTransactionProducerApp.getRandomTransaction("Hem","test-topic");
       assertEquals(record.key(), "Hem");
        ObjectMapper mapper=new ObjectMapper();
        try {
            JsonNode node=mapper.readTree(record.value());
            assertEquals(node.get("name").asText(),"Hem");
            assertTrue(node.get("amount").asInt() <100);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(record.value());
    }
}
