package com.github.tylerrice27.udemy.kafka.streams;


import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static junit.framework.TestCase.assertEquals;

public class WordCountAppTest {
    TopologyTestDriver testDriver;

    StringSerializer stringSerializer = new StringSerializer();

    TestInputTopic<String, String> inputTopic = testDriver.createInputTopic("word-count-input", stringSerializer, stringSerializer);

//    TestRecord<String,Long> recordFactory = new TestRecord<>(new StringSerializer(), new LongSerializer());

//    Before every single test the @Before annotation will run
    @Before
    public void setUpTopologyTestDriver(){
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-test");
//        You can put whatever you want for the server because it doesnt connect to Kafka
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

       WordCountApp wordCountApp = new WordCountApp();
       Topology topology = wordCountApp.createTopology();
        testDriver = new TopologyTestDriver(topology, config);
    }

    @After
    public void closeTestDriver(){
        testDriver.close();
    }

    public void pushNewInputRecord(String value){
        inputTopic.pipeInput(value);
//        inputTopic.pipeInput(value, (String) null);
    }

    @Test
    public void dummyTest(){
        String dummy = "Du" + "mmy";
        assertEquals(dummy, "Dummy");
    }

    @Test
    public void makeSureCountsAreCorrect(){
    String firstExample = "testing Kafka Streams";
    pushNewInputRecord(firstExample);
    }

    @Test
    public ProducerRecord<String,Long> readOutput(){
       return  TestOutputTopic<String, Long> outputTopic = testDriver.createOutputTopic("word-count-output", stringSerde.deserializer(), longSerde.deserializer();

    }

}
