package org.bakdata.kafka.challenge.customSerde.tfidfResultSerde;

import java.io.IOException;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.bakdata.kafka.challenge.model.TFIDFResult;
import org.codehaus.jackson.map.ObjectMapper;

public class TFIDFResultDeserializer implements Deserializer<TFIDFResult> {
    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
    }

    @Override
    public TFIDFResult deserialize(final String s, final byte[] data) {
        final ObjectMapper objectMapper = new ObjectMapper();
        if (data == null) {
            return null;
        }
        try {
            return objectMapper.readValue(data, TFIDFResult.class);
        } catch (final IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void close() {
    }
}
