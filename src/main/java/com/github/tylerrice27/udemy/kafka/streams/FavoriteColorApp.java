package com.github.tylerrice27.udemy.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class FavoriteColorApp {

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favorite-color");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    StreamsBuilder builder = new StreamsBuilder();

//    1- Read a KStream and extract the key
KStream<String, String> favoriteColorInput = builder.stream("favorite-color-input");

//    2 - Write to a Kafka topic. That topic should be a compacted topic
    favoriteColorInput.toStream().to("compacted-color-topic";)

//    3- The results can be read from the compacted Topic from Kafka as a KTable
KTable<String,String> favoriteColorKTable = builder.table("compacted-color-topic")
//    4 - From a KTable perform aggregation on the KTable (groupBy then count)
        .groupByKey()
        .count();
//    5 - write the results back to Kafka
favoriteColorKTable.toStream().to("final-color-topic");
    KTable<String, String> updatedColor = favoriteColorInput.flatMapValues()


}
