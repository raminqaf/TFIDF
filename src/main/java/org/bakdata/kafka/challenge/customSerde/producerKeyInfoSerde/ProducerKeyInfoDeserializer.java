package org.bakdata.kafka.challenge.costumSerde.producerKeyInfoSerde;

import org.apache.kafka.common.serialization.Deserializer;
import org.bakdata.kafka.challenge.model.Information;
import org.bakdata.kafka.challenge.model.ProducerKeyInfo;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Map;

public class ProducerKeyInfoDeserializer implements Deserializer<ProducerKeyInfo> {
    @Override public void configure(Map<String, ?> configs, boolean isKey) { }

    @Override public ProducerKeyInfo deserialize(String s, byte[] data) {
        ObjectMapper objectMapper = new ObjectMapper();
        if (data == null) return null;
        try {
            return objectMapper.readValue(data, ProducerKeyInfo.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override public void close() { }
}
