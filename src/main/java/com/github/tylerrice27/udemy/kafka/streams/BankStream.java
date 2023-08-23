package com.github.tylerrice27.udemy.kafka.streams;

import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class BankStream {

    public static void main(String[] args) {

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "bank-balance");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        StreamsBuilder builder = new StreamsBuilder();

//    1. Read from one topic in Kafka
        KStream<String, String> newBankTransaction = builder.stream("bank-input");

//    2. GroupByKey, because your topic already has the right key, Which is the person name
        KTable<String, Long> bankAmount = newBankTransaction.groupByKey()
//    3. Aggregate, to comput the bank balance
                .count(Named.as("Total"));
//    4. Send To in order to write the data back to Kafka

        bankAmount.toStream().to("final-balance");

        KafkaStreams streams = new KafkaStreams(builder.build(),config);
        streams.start();

        System.out.println(streams.toString());

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
