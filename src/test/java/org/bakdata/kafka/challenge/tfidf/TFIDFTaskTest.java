/*
 * MIT License
 *
 * Copyright (c) 2020 bakdata GmbH
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.bakdata.kafka.challenge.tfidf;

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
import org.bakdata.kafka.challenge.constant.IKafkaConstants;
import org.bakdata.kafka.challenge.customSerde.producerKeyInfoSerde.ProducerKeyInfoSerializer;
import org.bakdata.kafka.challenge.customSerde.tfidfResultSerde.TFIDFResultDeserializer;
import org.bakdata.kafka.challenge.model.ProducerKeyInfo;
import org.bakdata.kafka.challenge.model.TFIDFResult;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TFIDFTaskTest {

    private TopologyTestDriver testDriver = null;
    private TestInputTopic<ProducerKeyInfo, String> inputTopic = null;
    private TestOutputTopic<String, TFIDFResult> outputTopic = null;

    @Before
    public void setup() {
        final StreamsBuilder builder = new StreamsBuilder();
        TFIDFTask.createTFIDFStream(builder);
        final Properties props = TFIDFTask.getStreamsConfiguration();
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
    public void testPipeline() {
        final ProducerKeyInfo producerKeyInfo = new ProducerKeyInfo("document2.txt", 2);
        final String fileContent = "this is an example example";
        final TFIDFResult tfidfResult = new TFIDFResult("document2.txt", 0.0d, 2);

        this.inputTopic.pipeInput(producerKeyInfo, fileContent);
        final KeyValue<String, TFIDFResult> kv = this.outputTopic.readKeyValue();

        assertThat(kv, equalTo(new KeyValue<>("this", tfidfResult)));

        assertThat(this.outputTopic.isEmpty(), is(false));

    }

}
