package com.github.tylerrice27.udemy.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class FavoriteColorApp {

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favorite-color");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

//        We disable the cache to demonstrate all the "steps" involved in the transformation of data - not recommended in prod
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");


        StreamsBuilder builder = new StreamsBuilder();

//    1- Read a KStream
        KStream<String, String> favoriteColorInput = builder.stream("color-input");
        KStream<String, String> onlyColors = favoriteColorInput
//    2 - Filter Bad Values, Must have a , This is good so your data doesnt crash
                .filter((key, value) -> value.contains(","))
//    3 - SelectKey that will be the user Id. This tells it which word is the key
                .selectKey((key, value) -> value.split(",")[0].toLowerCase())
//    4 - MapValues we get the color from the value and (lowercase for safety). We also split on the , and choose
//        one index to right
                .mapValues(value -> value.split(",")[1].toLowerCase())
//    5 - Filter to remove bad colors. (could be a data sanitization step)
                .filter((userAsAKey, valueAsAColor) -> Arrays.asList("red", "blue", "green").contains(valueAsAColor));
//    6 - Write to Kafka as intermediary topic
        onlyColors.to("user-key-and-color");
//    7 - Read from Kafka as a KTable (KTable)

//KTable<String, String> compactedColors = builder.table("middle-topic")
////    8 - GroupBy colors
//        .groupBy(key,value)
////    9 - Count to count colors occurrences (KTable)
//        .count()
////    10 - Write to Kafka as final topic
//        table.to("color-output")
    }
}
