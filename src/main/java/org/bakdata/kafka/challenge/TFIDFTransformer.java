package org.bakdata.kafka.challenge;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.bakdata.kafka.challenge.model.Information;
import org.bakdata.kafka.challenge.model.TFIDFResult;

public class TFIDFTransformer implements Transformer<String, Information, KeyValue<String, TFIDFResult>> {

    private KeyValueStore<String, Double> wordOccurrences = null;

    @Override
    public void init(final ProcessorContext processorContext) {

        // retrieve the key-value store named "Counts"
        this.wordOccurrences =
                (KeyValueStore<String, Double>) processorContext.getStateStore(IKeyValueStore.PERSISTENT_KV_OVERALL_WORD_COUNT);
    }

    @Override
    public KeyValue<String, TFIDFResult> transform(final String word, final Information information) {

        final double tf = information.getTermFrequency();
        final String documentName = information.getDocumentName();
        final double overallDocumentCount = information.getOverallDocumentCount();

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

    @Override
    public void close() {

    }
}
