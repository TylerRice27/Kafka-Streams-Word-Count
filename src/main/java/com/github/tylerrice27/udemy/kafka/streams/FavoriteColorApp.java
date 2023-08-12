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

//    1- Read a KStream
KStream<String, String> favoriteColorInput = builder.stream("color-input");
//    2 - Filter Bad Values
KStream<String, String> onlyColors = favoriteColorInput.filter((key, value) -> value == "")
//    3 - SelectKey that will be the user Id
        .selectKey((ignoredKey, newKey) -> newKey)
//    4 - MapValues to extract the color (as lowercase)
        .mapValues(onlyColor -> onlyColor.toLowerCase())
//    5 - Filter to remove bad colors. NOTE come back to this need to filter more
        .filter((key, value) -> value == "red")
//    6 - Write to Kafka as intermediary topic
    KStream<String>
//    7 - Read from Kafka as a KTable (KTable)

//    8 - GroupBy colors

//    9 - Count to count colors occurrences (KTable)

//    10 - Write to Kafka as final topic


}
