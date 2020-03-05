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

package org.bakdata.kafka.challenge.transformer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Iterator;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.bakdata.kafka.challenge.constant.IKafkaConstants;
import org.bakdata.kafka.challenge.constant.IKeyValueStore;
import org.junit.Before;
import org.junit.Test;

public class TFIDFTransformerTest {

    private final Transformer transformerUnderTest = new TFIDFTransformer();
    private MockProcessorContext context = null;

    @Before
    public void setup() {

        // setup test driver
        final Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "unit-test");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConstants.KAFKA_BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        this.context = new MockProcessorContext(props);

        this.transformerUnderTest.init(this.context);

        final KeyValueStore<String, Double> store =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(IKeyValueStore.PERSISTENT_KV_OVERALL_WORD_COUNT),
                        Serdes.String(),
                        Serdes.Double()
                )
                        .withLoggingDisabled() // Changelog is not supported by MockProcessorContext.
                        .build();
        store.init(this.context, store);

        this.context.register(store, null);
    }

    @Test
    public void testTransformer() {
        this.transformerUnderTest.transform("example", "0.429@document2.txt@2");

        final Iterator<MockProcessorContext.CapturedForward> forwarded = this.context.forwarded().iterator();

        assertEquals(new KeyValue<>("key", 1.0d), forwarded.next().keyValue());

        assertFalse(forwarded.hasNext());

        this.context.resetForwards();

        assertEquals(0, this.context.forwarded().size());
    }
}
