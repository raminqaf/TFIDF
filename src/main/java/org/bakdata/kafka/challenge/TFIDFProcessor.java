package org.bakdata.kafka.challenge;

import static java.lang.Math.log10;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class TFIDFProcessor implements Processor<String, String> {

    private ProcessorContext context = null;
    private List<String> documents = null;
    private Map<String, Set<String>> wordDocumentMap = null;
    private Map<String, Double> mapTF = null;

    private static KeyValueStore<String, Double> overallWordCount = null;
    private static Double documentsCount = null;


    @Override
    public void init(final ProcessorContext processorContext) {
        this.context = processorContext;
        this.documents = new ArrayList<>();
        this.wordDocumentMap = new HashMap<>();
        this.mapTF = new HashMap<>();
        // retrieve the key-value store named "Counts"
        overallWordCount = (KeyValueStore) this.context.getStateStore("idf");
    }

    @Override
    public void process(final String word, final String tfDocNameDocCount) {
        final double tf = Double.parseDouble(tfDocNameDocCount.split("@")[0]);
        final String docName = tfDocNameDocCount.split("@")[1];
        this.mapTF.put(word + "@" + docName, tf);
        //documentsCount = Double.parseDouble(tfDocNameDocCount.split("@")[2]);

        this.calculateDocumentCount(docName);
        final Set<String> setMap = this.wordDocumentMap.get(word);
        if (setMap != null) {
            setMap.add(docName);
            this.wordDocumentMap.put(word, setMap);
        } else {
            final Set<String> set = new HashSet<>();
            set.add(docName);
            this.wordDocumentMap.put(word, set);
        }

        this.context.forward(word, tfDocNameDocCount);
        this.context.commit();

    }

    @Override
    public void close() {
        System.out.println("Close called");

        final Map<String, Double> mapIDF = new HashMap<>();
        this.wordDocumentMap.forEach((word, documents) -> {
            final double documentFrequency = documents.size();
            final double idf = log10(documentsCount / documentFrequency);
            documents.forEach((documentName) -> {
                mapIDF.put(word + "@" + documentName, idf);
            });
        });

        final FileWriter myWriter;
        try {
            myWriter = new FileWriter("Data/output.csv");
            myWriter.write("name,tf,idf,tfidf" + "\n");

            mapIDF.forEach((word, idf) -> {
                if (this.mapTF.containsKey(word)) {
                    final double tf = this.mapTF.get(word);
                    final double tfidf = idf * tf;
                    final String out = word + "," + tf + "," + idf + "," + tfidf;
                    System.out.println(out);
                    try {
                        myWriter.write(out + "\n");
                    } catch (final IOException e) {
                        e.printStackTrace();
                    }
                }
            });

            myWriter.close();

        } catch (final IOException e) {
            e.printStackTrace();
        }
    }

    private void calculateDocumentCount(final String docName) {
        if (!this.documents.contains(docName)) {
            documentsCount =
                    overallWordCount.get("documentCount") == null ? 0.0d : overallWordCount.get("documentCount");
            this.documents.add(docName);
            documentsCount = (double) this.documents.size();
            overallWordCount.put("documentCount", documentsCount);
        }
    }
}
