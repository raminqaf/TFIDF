package org.bakdata.kafka.challenge;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Properties;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.bakdata.kafka.challenge.customSerde.producerKeyInfoSerde.ProducerKeyInfoSerializer;
import org.bakdata.kafka.challenge.customSerde.tfidfResultSerde.TFIDFResultDeserializer;
import org.bakdata.kafka.challenge.model.ProducerKeyInfo;
import org.bakdata.kafka.challenge.model.TFIDFResult;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TFIDFApplicationTest {

    private TopologyTestDriver testDriver = null;
    private TestInputTopic<ProducerKeyInfo, String> inputTopic = null;
    private TestOutputTopic<String, TFIDFResult> outputTopic = null;

    @Before
    public void setup() {
        final StreamsBuilder builder = new StreamsBuilder();
        TFIDFApplication.createWordCountStream(builder);
        final Properties props = TFIDFApplication.getStreamsConfiguration();
        this.testDriver = new TopologyTestDriver(builder.build(), props);
        this.inputTopic = this.testDriver
                .createInputTopic(IKafkaConstants.INPUT_TOPIC, new ProducerKeyInfoSerializer(), new StringSerializer());
        this.outputTopic = this.testDriver.createOutputTopic(IKafkaConstants.OUTPUT_TOPIC, new StringDeserializer(),
                new TFIDFResultDeserializer());
    }

    @After
    public void tearDown() {
        this.testDriver.close();
    }

    @Test
    public void shouldNotUpdateStoreForLargerValue() {
        final ProducerKeyInfo producerKeyInfo = new ProducerKeyInfo("document2.txt", 2);
        final String fileContent = "this is an example example";
        final TFIDFResult tfidfResult = new TFIDFResult("document2.txt", 0.0d, 2);

        this.inputTopic.pipeInput(producerKeyInfo, fileContent);
        final KeyValue<String, TFIDFResult> kv = this.outputTopic.readKeyValue();

        assertThat(kv, equalTo(new KeyValue<>("this", tfidfResult)));

        assertThat(this.outputTopic.isEmpty(), is(false));

    }

}
