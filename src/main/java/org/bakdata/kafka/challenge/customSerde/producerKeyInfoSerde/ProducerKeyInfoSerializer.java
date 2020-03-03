package org.bakdata.kafka.challenge.customSerde.producerKeyInfoSerde;

import java.util.Map;
import org.apache.kafka.common.serialization.Serializer;
import org.bakdata.kafka.challenge.customSerde.SerializeData;
import org.bakdata.kafka.challenge.model.ProducerKeyInfo;

public class ProducerKeyInfoSerializer implements Serializer<ProducerKeyInfo> {
    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
    }

    @Override
    public byte[] serialize(final String s, final ProducerKeyInfo data) {
        return SerializeData.Serialize(data);
    }

    @Override
    public void close() {
    }
}
