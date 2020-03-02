package org.bakdata.kafka.challenge;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.bakdata.kafka.challenge.model.Information;
import org.bakdata.kafka.challenge.model.TFIDFResult;

public class TFIDFTransformer implements Transformer<String, Information, KeyValue<String, TFIDFResult>> {

    private ProcessorContext context;
    private KeyValueStore<String, Double> wordOccurrences;

    @Override public void init(ProcessorContext processorContext) {
        this.context = processorContext;

        // retrieve the key-value store named "Counts"
        wordOccurrences = (KeyValueStore<String, Double>) context.getStateStore(IKeyValueStore.PERSISTENT_KV_OVERALL_WORD_COUNT);
    }

    @Override public KeyValue<String, TFIDFResult> transform(String word, Information information) {

        double tf = information.getTermFrequency();
        String documentName = information.getDocumentName();
        double overallDocumentCount = information.getOverallDocumentCount();


        Double occurrences = this.wordOccurrences.get(word);
        // handle missing entry
        if (occurrences == null) {
            occurrences = 0d;
        }
        occurrences += 1;
        // update store
        this.wordOccurrences.put(word, occurrences);

        final double idf = Math.log10(overallDocumentCount / occurrences);
        final double tfidf = tf * idf;

        final TFIDFResult result = new TFIDFResult(documentName, tfidf, overallDocumentCount);

        return new KeyValue<>(word, result);
    }

    @Override public void close() {

    }
}
