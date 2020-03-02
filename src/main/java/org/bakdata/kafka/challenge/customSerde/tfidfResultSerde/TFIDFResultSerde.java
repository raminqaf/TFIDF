package org.bakdata.kafka.challenge.costumSerde.tfidfResultSerde;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.bakdata.kafka.challenge.model.TFIDFResult;

import java.util.Map;

public class TFIDFResultSerde implements Serde<TFIDFResult> {

    private final Serde<TFIDFResult> inner;

    public TFIDFResultSerde() {
        this.inner = Serdes.serdeFrom(new TFIDFResultSerializer(), new TFIDFResultDeserializer());
    }

    @Override public void configure(Map<String, ?> configs, boolean isKey) {
        this.inner.configure(configs, isKey);
    }

    @Override public void close() {
        this.inner.close();
    }

    @Override public Serializer<TFIDFResult> serializer() {
        return this.inner.serializer();
    }

    @Override public Deserializer<TFIDFResult> deserializer() {
        return this.inner.deserializer();
    }
}
