package com.github.tylerrice27.udemy.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class StreamsStarterApp {
    public static void main(String[] args) {

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder builder = new StreamsBuilder();

//        1 - Stream from Kafka

//        NOTE with KStream the String on the left is my key and the String on the right is the value of the data
        KStream<String, String> wordCountInput = builder.stream("word-count-input");

//        2 - map values to lowercase

        KTable<String, Long> wordCounts = wordCountInput.mapValues(streamWords -> streamWords.toLowerCase())

//        NOTE on the line about I did not end it with a ; instead I am able to chain the function with .Operation
//        3 - flatmap values split by space
//        NOTE this line allows you take the steam of messages and split on the space and put them in array of multiple messages.
                .flatMapValues(lowerCaseStreamWords -> Arrays.asList(lowerCaseStreamWords.split(" ")))

//        4 - select key to apply a key (This discards the old key)
                .selectKey((ignoredKey, keyAsTheWord) -> keyAsTheWord)
//        5 - Group by key before aggregation
                .groupByKey()

//        6 - count occurences in each group
                .count(Named.as("Counts"));
//        7 - to in order to write the results back to Kafka
//        NOTE Kafka is a strongly typed library and you may get a lot of typecast errors if you don't specify the right types between Kafka and Streams
//        wordCounts.to(Serdes.String(), Serdes.Long(), "word-count-output");

        wordCounts.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();




    }
}
