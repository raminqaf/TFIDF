package org.bakdata.kafka.challenge.costumSerde.tfidfResultSerde;

import org.apache.kafka.common.serialization.Deserializer;
import org.bakdata.kafka.challenge.model.TFIDFResult;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Map;

public class TFIDFResultDeserializer implements Deserializer<TFIDFResult> {
    @Override public void configure(Map<String, ?> configs, boolean isKey) { }

    @Override public TFIDFResult deserialize(String s, byte[] data) {
        ObjectMapper objectMapper = new ObjectMapper();
        if (data == null) return null;
        try {
            return objectMapper.readValue(data, TFIDFResult.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override public void close() { }
}
