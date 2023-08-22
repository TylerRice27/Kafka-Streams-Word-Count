package com.github.tylerrice27.udemy.kafka.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class BankProducerTest {

    @Test
    public void randomTransactionTest(){
        ProducerRecord<String,String > record = BankProducer.randomTransaction("Tyler");
        String key = record.key();
        String value = record.value();

        assertEquals(key, "Tyler");

        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode node =  mapper.readTree(value);
            assertEquals(node.get("name").asText(), "Tyler");
            assertTrue("Amount should be less than 100", node.get("amount").asInt() < 100);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        System.out.println(value);


    }

}
