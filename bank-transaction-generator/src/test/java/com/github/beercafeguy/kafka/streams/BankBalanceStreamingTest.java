package com.github.beercafeguy.kafka.streams;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import java.time.Instant;

public class BankBalanceStreamingTest {
    @Test
    @Ignore
    public void testCalculator(){
        ObjectNode balance = JsonNodeFactory.instance.objectNode();
        balance.put("count",10);
        balance.put("balance",200);
        balance.put("time",Instant.ofEpochMilli(10000000L).toString());

        ObjectNode transaction = JsonNodeFactory.instance.objectNode();
        balance.put("name","Hem");
        balance.put("amount",100);
        balance.put("time",Instant.ofEpochMilli(10000100L).toString());

        assertEquals(BankBalanceStreamingApp.getBalance(transaction,balance).get("balance").asInt(),300);
    }
}
