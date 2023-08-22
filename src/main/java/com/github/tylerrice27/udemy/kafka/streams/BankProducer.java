package com.github.tylerrice27.udemy.kafka.streams;

import org.apache.kafka.clients.producer.*;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.Properties;


public class BankProducer {

    private static final Logger log = LoggerFactory.getLogger(BankProducer.class.getSimpleName());
    public static void main(String[] args) {

        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.Serializer");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.Serializer");
        config.put(ProducerConfig.ACKS_CONFIG, 1);

        KafkaProducer<String,Integer> producer = new KafkaProducer<>(config);

        String topic = "BankTopic";
        String key = "Name";
        Integer value = 0;


        ProducerRecord<String, Integer> moneyMessage = new ProducerRecord<>(topic, key, value);


        producer.send(moneyMessage, new Callback(){

                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e == null){
                            log.info("Key:" + key + "|Value:" + value);
                        }else{
                            log.error("Error while producing the random bank number");
                        }
                    }
                });
        try{
            Thread.sleep(500);
        }catch (InterruptedException e){
            throw new RuntimeException(e);
        }


        producer.flush();

        producer.close();

    }


}
