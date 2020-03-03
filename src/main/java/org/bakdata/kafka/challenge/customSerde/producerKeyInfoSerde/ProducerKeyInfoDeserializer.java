package org.bakdata.kafka.challenge.customSerde.producerKeyInfoSerde;

import java.io.IOException;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.bakdata.kafka.challenge.model.ProducerKeyInfo;
import org.codehaus.jackson.map.ObjectMapper;

public class ProducerKeyInfoDeserializer implements Deserializer<ProducerKeyInfo> {
    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
    }

    @Override
    public ProducerKeyInfo deserialize(final String s, final byte[] data) {
        final ObjectMapper objectMapper = new ObjectMapper();
        if (data == null) {
            return null;
        }
        try {
            return objectMapper.readValue(data, ProducerKeyInfo.class);
        } catch (final IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void close() {
    }
}
