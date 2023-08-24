package com.github.tylerrice27.udemy.kafka.streams;

import jdk.nashorn.internal.objects.Global;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class UserEventEnricherApp {

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "user-event-enricher-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder builder = new StreamsBuilder();

//        Get a global table out of Kafka. This table will be replicated on each Kafka Streams application
//        the key of our GlobalKTable is the userID
//        NOTE you can use the builder variable up above multiple times
//        GlobalTable of user data
        GlobalKTable<String, String> usersGlobalTable = builder.globalTable("user-table");

//        purchase Stream
        KStream<String, String> userPurchases = builder.stream("user-purchases");

//        Enrich the Stream. Which our output of a KStream to GlobalKTable is KStream
        KStream<String,String> userPurchasesEnrichedJoin =
                userPurchases.join(usersGlobalTable,  // We want to join the user Purchaes with GlobalTable info
//                        This map is standard for joins. Depending on what you are trying to join
                        (key, value) -> key,   // This line is doing a map (key, value) from the UserPurchase stream then joins on the key from the GlobalKTable that match
//                        Value of the stream and the value of the Table that is renamed to userPurchase and userInfo
                        (userPurchase, userInfo) -> "Purchase=" + userPurchase + ",UserInfo=[" + userInfo + "]"
                );

        userPurchasesEnrichedJoin.to("user-purchases-enriched-inner-join");

        KStream<String, String> userPurchasesEnrichedLeftJoin =
                userPurchases.leftJoin(usersGlobalTable,
                        (key, value) -> key,
                        (userPurchase, userInfo) -> {
//                    Because I am doing a left join and getting back all the purchases if it does not have
//                     a user asscociated to that purchase I need to account for that
                            if (userInfo != null) {
                                return "Purchase=" + userPurchase + ",UserInfo=[" + userInfo + "]";
                            } else {
                                return "Purchase=" + userPurchase + ",UserInfo=null";
                            }
                        });
        userPurchasesEnrichedLeftJoin.to("user-purchases-enriched-left-join");


    }


}
