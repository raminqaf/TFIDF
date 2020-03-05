/*
 * MIT License
 *
 * Copyright (c) 2020 bakdata GmbH
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.bakdata.kafka.challenge.transformer;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.bakdata.kafka.challenge.constant.IKeyValueStore;
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
            occurrences = 0.0d;
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
