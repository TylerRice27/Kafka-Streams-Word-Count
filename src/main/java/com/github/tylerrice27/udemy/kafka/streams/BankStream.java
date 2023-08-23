package com.github.tylerrice27.udemy.kafka.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;


import java.time.Instant;
import java.util.Properties;

public class BankStream {

    public static void main(String[] args) {

        Properties config = new Properties();
//        A better name for Application Id could have bank-balance-application
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "bank-balance");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
//       REMEMBER to add Exactly Once config
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
//        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerializer.class);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

//        Create your own Serializer and Deserializer for a JSON because it doesn't come prepackaged
//        This might be deprecated!!!!!!!!



        class JsonSerializer implements Serializer<JsonNode> {
            private final ObjectMapper objectMapper = new ObjectMapper();

            @Override
            public byte[] serialize(String topic, JsonNode data) {
                try {
                    return objectMapper.writeValueAsBytes(data);
                } catch (JsonProcessingException e) {
                    throw new SerializationException("Error serializing JSON", e);
                }
            }
        }

        class JsonDeserializer implements Deserializer<JsonNode> {
            private final ObjectMapper objectMapper = new ObjectMapper();

            @Override
            public JsonNode deserialize(String s, byte[] bytes) {
                return null;
            }
        }

        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        Serde<JsonNode> jsonSerde = Serdes.serdeFrom(new JsonSerializer(), new JsonDeserializer());


        StreamsBuilder builder = new StreamsBuilder();

//    1. Read from one topic in Kafka
        KStream<String, JsonNode> newBankTransaction = builder.stream("bank-input");

//        Create the initial JSON object for balance
        ObjectNode startingBalance = JsonNodeFactory.instance.objectNode();
        startingBalance.put("count", 0);
        startingBalance.put("balance", 0);
        startingBalance.put("time", Instant.ofEpochMilli(0L).toString());

//    2. GroupByKey, because your topic already has the right key, Which is the person name
        KTable<String, JsonNode> bankAmount = newBankTransaction
                .groupByKey()
//    3. Aggregate, to compute the bank balance
        .aggregate(
                () -> startingBalance,
//                TO DO COME back here and try to figure out this line
//                My types do not match up correctly and also I think balance is missing completely
                (key, transaction, balance) -> newBalance(transaction, balance)
//                jsonSerde,
//                "bank-balance-agg"
        );
//                .count(Named.as("Total"));
//    4. Send To in order to write the data back to Kafka

        bankAmount.toStream().to("bank-output", Produced.with(Serdes.String(), jsonSerde));

        KafkaStreams streams = new KafkaStreams(builder.build(),config);
        streams.cleanUp();
        streams.start();

        System.out.println(streams.toString());

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }


    private static JsonNode newBalance(JsonNode transaction, JsonNode balance){
//        Create a new balance JSON object
        ObjectNode newBalance = JsonNodeFactory.instance.objectNode();
        newBalance.put("count", balance.get("count").asInt()+ 1);
        newBalance.put("balance", balance.get("balance").asInt() + transaction.get("amount").asInt());
// Time of the object which gives us our max value
        Long balanceEpoch = Instant.parse(balance.get("time").asText()).toEpochMilli();
        Long transactionEpoch = Instant.parse(transaction.get("time").asText()).toEpochMilli();
        Instant newBalanceInstant = Instant.ofEpochMilli(Math.max(balanceEpoch, transactionEpoch));
        newBalance.put("time", newBalanceInstant.toString());
        return newBalance;


    }

}
