package org.bakdata.kafka.challenge;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.bakdata.kafka.challenge.IKafkaConstants;
import org.bakdata.kafka.challenge.IKeyValueStore;
import org.bakdata.kafka.challenge.TFIDFTransformer;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TFIDFTransformerTest {

    private final Transformer transformerUnderTest = new TFIDFTransformer();
    private MockProcessorContext context;

    @Before
    public void setup() {

        // setup test driver
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "unit-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConstants.KAFKA_BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        context = new MockProcessorContext(props);

        transformerUnderTest.init(context);

        final KeyValueStore<String, Double> store =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(IKeyValueStore.PERSISTENT_KV_OVERALL_WORD_COUNT),
                        Serdes.String(),
                        Serdes.Double()
                )
                        .withLoggingDisabled() // Changelog is not supported by MockProcessorContext.
                        .build();
        store.init(context, store);

        context.register(store, null);
    }

    @Test
    public void testTransformer() {
        transformerUnderTest.transform("example", "0.429@document2.txt@2");

        final Iterator<MockProcessorContext.CapturedForward> forwarded = context.forwarded().iterator();

        assertEquals(forwarded.next().keyValue(), new KeyValue<>("key", 1d));

        assertFalse(forwarded.hasNext());

        // you can reset forwards to clear the captured data. This may be helpful in constructing longer scenarios.
        context.resetForwards();

        assertEquals(context.forwarded().size(), 0);
    }
}
