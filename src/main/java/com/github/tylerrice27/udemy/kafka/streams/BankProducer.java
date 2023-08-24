package com.github.tylerrice27.udemy.kafka.streams;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;


public class BankProducer {

    private static final Logger log = LoggerFactory.getLogger(BankProducer.class.getSimpleName());
    public static void main(String[] args) {

        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
//        This enables Exactly Once Delivery Semantics
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.ACKS_CONFIG, "all");
//        Three retries in case something goes wrong so I dont loose data
        config.put(ProducerConfig.RETRIES_CONFIG, "3");
//        Linger MS is wait time. This is not great for production but this will send data very fast
        config.put(ProducerConfig.LINGER_MS_CONFIG, "1");


        KafkaProducer<String, String> producer = new KafkaProducer<>(config);


//        My main loop that produces the message
        int i = 0;
        while (true) {
            try {
                producer.send(randomTransaction("Tyler"));
                Thread.sleep(100);
                producer.send(randomTransaction("David"));
                Thread.sleep(100);
                producer.send(randomTransaction("Mike"));
                i += 1;
            } catch (InterruptedException e) {
                break;
            }

        }
        producer.flush();
        producer.close();
    }


        public static ProducerRecord<String,String> randomTransaction(String name){
//            Creates an empty JSON {} because of the dependancy library I brought in
            ObjectNode transaction = JsonNodeFactory.instance.objectNode();
//            This creates a random transaction 0 to 100
            Integer amount = ThreadLocalRandom.current().nextInt(0,100);

//            Instant.now() is to get the current time using Java
            Instant now = Instant.now();
//            Write the data to the JSON document
            transaction.put("name", name);
            transaction.put("amount", amount);
            transaction.put("time", now.toString());
//           Return the message to the topic, key, transaction == value which is a JSON then toString()
            return new ProducerRecord<>("bank-input", name, transaction.toString());
        }

    }
