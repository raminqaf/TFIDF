package org.bakdata.kafka.challenge.customSerde;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;

public final class SerializeData {
    private SerializeData() {
    }

    public static byte[] Serialize(final Object data) {
        final ObjectMapper objectMapper = new ObjectMapper();
        if (data == null) {
            return null;
        }
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (final IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
