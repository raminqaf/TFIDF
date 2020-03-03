package org.bakdata.kafka.challenge.customSerde.producerKeyInfoSerde;

import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.bakdata.kafka.challenge.model.ProducerKeyInfo;

public class ProducerKeyInfoSerde implements Serde<ProducerKeyInfo> {

    private final Serde<ProducerKeyInfo> inner;

    public ProducerKeyInfoSerde() {
        this.inner = Serdes.serdeFrom(new ProducerKeyInfoSerializer(), new ProducerKeyInfoDeserializer());
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        this.inner.configure(configs, isKey);
    }

    @Override
    public void close() {
        this.inner.close();
    }

    @Override
    public Serializer<ProducerKeyInfo> serializer() {
        return this.inner.serializer();
    }

    @Override
    public Deserializer<ProducerKeyInfo> deserializer() {
        return this.inner.deserializer();
    }
}
