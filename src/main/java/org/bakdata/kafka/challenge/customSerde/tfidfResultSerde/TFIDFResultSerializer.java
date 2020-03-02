package org.bakdata.kafka.challenge.costumSerde.tfidfResultSerde;

import org.apache.kafka.common.serialization.Serializer;
import org.bakdata.kafka.challenge.model.TFIDFResult;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Map;

public class TFIDFResultSerializer implements Serializer<TFIDFResult> {
    @Override public void configure(Map<String, ?> configs, boolean isKey) { }

    @Override public byte[] serialize(String s, TFIDFResult data) {
        ObjectMapper objectMapper = new ObjectMapper();
        if (data == null) return null;
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override public void close() { }
}
