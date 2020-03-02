package org.bakdata.kafka.challenge;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class TFIDFApplicationTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, String> outputTopic;

    @Before
    public void setup() {
        StreamsBuilder builder = new StreamsBuilder();
        TFIDFApplication.createWordCountStream(builder);
        Properties props = TFIDFApplication.getStreamsConfiguration();
        testDriver = new TopologyTestDriver(builder.build(), props);
        inputTopic = testDriver.createInputTopic(IKafkaConstants.INPUT_TOPIC, new StringSerializer(), new StringSerializer());
        outputTopic = testDriver.createOutputTopic(IKafkaConstants.OUTPUT_TOPIC, new StringDeserializer(), new StringDeserializer());
    }

    @After
    public void tearDown() {
        testDriver.close();
    }

    @Test
    public void shouldNotUpdateStoreForLargerValue() {
        inputTopic.pipeInput("document2.txt-2", "this is an example example");

        KeyValue<String, String> kv = outputTopic.readKeyValue();

        assertThat(kv, equalTo(new KeyValue<>("hello", 1L)));
        //No more output in topic
        assertThat(outputTopic.isEmpty(), is(true));

    }

}
