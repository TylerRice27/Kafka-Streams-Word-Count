package com.github.tylerrice27.udemy.kafka.streams;


import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static junit.framework.TestCase.assertEquals;

public class WordCountAppTest {
    TopologyTestDriver testDriver;

    StringSerializer stringSerializer = new StringSerializer();

    TestInputTopic<String, String> inputTopic = testDriver.createInputTopic("word-count-input", stringSerializer, stringSerializer);

    TestOutputTopic<String, Long> outputTopic = testDriver.createOutputTopic("word-count-output", new StringDeserializer(), new LongDeserializer());

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

//    Always close your testDriver in a After block or your test will if you run second time because they won't be cleaned up
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
    TestOutputTopic<String, Long> firstOutput = readOutput();
// TO DO come back to this OutputVerifier wont import so it might be something different to test now but cant find alternative
   OutputVerifier.compareKeyValue(readOutput(), "testing", 1L);
   OutputVerifier.compareKeyValue(readOutput(), "kafka", 1L);
   OutputVerifier.compareKeyValue(readOutput(), "streams", 1L);

   String secondExample = "testing Kafka again";
   pushNewInputRecord(secondExample);
   OutputVerifier.compareKeyValue(readOutput(), "testing", 2L);
   OutputVerifier.compareKeyValue(readOutput(), "kafka", 2L);
   OutputVerifier.compareKeyValue(readOutput(), "again", 1L);

    }

    @Test
    public void makeSureWordsBecomeLowercase(){
        String upperCaseString = "KAFKA kafka Kafka";
        pushNewInputRecord(upperCaseString);
        OutputVerifier.compareKeyValue(readOutput(), "kafka", 1L);
        OutputVerifier.compareKeyValue(readOutput(), "kafka", 2L);
        OutputVerifier.compareKeyValue(readOutput(), "kafka", 3L);

    }

    @Test
    public TestOutputTopic<String,Long> readOutput(){
     return testDriver.createOutputTopic("word-count-output", new StringDeserializer(), new LongDeserializer());

    }

}
