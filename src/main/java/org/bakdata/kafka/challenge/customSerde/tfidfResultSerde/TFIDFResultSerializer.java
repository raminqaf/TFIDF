package org.bakdata.kafka.challenge.customSerde.tfidfResultSerde;

import java.util.Map;
import org.apache.kafka.common.serialization.Serializer;
import org.bakdata.kafka.challenge.customSerde.SerializeData;
import org.bakdata.kafka.challenge.model.TFIDFResult;

public class TFIDFResultSerializer implements Serializer<TFIDFResult> {
    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
    }

    @Override
    public byte[] serialize(final String s, final TFIDFResult data) {
        return SerializeData.Serialize(data);
    }

    @Override
    public void close() {
    }
}
