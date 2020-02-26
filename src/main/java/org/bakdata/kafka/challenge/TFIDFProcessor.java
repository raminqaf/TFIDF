package org.bakdata.kafka.challenge;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.List;

public class TFIDFProcessor implements Processor<String, String> {

    private ProcessorContext context;
    private List<String> documents;
    public static KeyValueStore<String, String> kvStore;
    public static Long documentsCount;

    @Override public void init(ProcessorContext processorContext) {
        this.context = processorContext;
        this.documents = new ArrayList<>();
        // retrieve the key-value store named "Counts"
        kvStore = (KeyValueStore) context.getStateStore("documentCount");
    }

    @Override public void process(String s, String s2) {
        if(!documents.contains(s)) {
            if(kvStore.get("documentCount") == null) {
                documentsCount = 0L;
            } else {
                documentsCount = Long.valueOf(kvStore.get("documentCount"));
            }
            documentsCount++;
            documents.add(s);
            kvStore.put("documentCount", documentsCount.toString());
        }

        context.forward(s, s2);
        context.commit();

    }

    @Override public void close() {

    }
}
