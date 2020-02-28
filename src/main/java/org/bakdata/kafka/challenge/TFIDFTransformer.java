package org.bakdata.kafka.challenge;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class TFIDFTransformer implements Transformer<String, String, KeyValue<String, String>> {

    private ProcessorContext context;
    private KeyValueStore<String, Double> wordOccurrences;

    @Override public void init(ProcessorContext processorContext) {
        this.context = processorContext;

        // retrieve the key-value store named "Counts"
        wordOccurrences = (KeyValueStore<String, Double>) context.getStateStore("idf");
    }

    @Override public KeyValue<String, String> transform(String word, String tfDocNameDocCount) {

        double tf = Double.parseDouble(tfDocNameDocCount.split("@")[0]);
        String documentName = tfDocNameDocCount.split("@")[1];
        double overallDocumentCount = Double.parseDouble(tfDocNameDocCount.split("@")[2]);


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

//        final TfidfResult result = new TfidfResult(information.getDocument(), tfidf, overallDocumentCount);

        return new KeyValue<>(word, documentName + "@" + tf + "," + idf + "," + tfidf);
    }

    @Override public void close() {

    }
}
