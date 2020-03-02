package org.bakdata.kafka.challenge.costumSerde.producerKeyInfoSerde;

import org.apache.kafka.common.serialization.Serializer;
import org.bakdata.kafka.challenge.model.ProducerKeyInfo;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Map;

public class ProducerKeyInfoSerializer implements Serializer<ProducerKeyInfo> {
    @Override public void configure(Map<String, ?> configs, boolean isKey) { }

    @Override public byte[] serialize(String s, ProducerKeyInfo data) {
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
