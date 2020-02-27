package org.bakdata.kafka.challenge;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.lang.Math.log10;

public class TFIDFProcessor implements Processor<String, String> {

    private ProcessorContext context;
    private List<String> documents;
    private Map<String, Set<String>> wordDocumentMap;
    private Map<String, String> tfIdfMap;
    private Map<String, Double> mapTF;

    public static KeyValueStore<String, Double> overallWordCount;
    public static Double documentsCount;


    @Override public void init(ProcessorContext processorContext) {
        this.context = processorContext;
        this.documents = new ArrayList<>();
        this.wordDocumentMap = new HashMap<>();
        this.tfIdfMap = new HashMap<>();
        this.mapTF = new HashMap<>();
        // retrieve the key-value store named "Counts"
        overallWordCount = (KeyValueStore) context.getStateStore("idf");
    }

    @Override public void process(String word, String tfDocNameDocCount) {
        double tf = Double.parseDouble(tfDocNameDocCount.split("@")[0]);
        String docName = tfDocNameDocCount.split("@")[1];
        mapTF.put(word + "@" + docName, tf);
        //documentsCount = Double.parseDouble(tfDocNameDocCount.split("@")[2]);


        calculateDocumentCount(docName);
        Set<String> setMap = wordDocumentMap.get(word);
        if (setMap != null) {
            setMap.add(docName);
            wordDocumentMap.put(word, setMap);
        } else {
            Set<String> set = new HashSet<>();
            set.add(docName);
            wordDocumentMap.put(word, set);
        }

        context.forward(word, tfDocNameDocCount);
        context.commit();

    }

    @Override public void close() {
        System.out.println("Close called");

        Map<String, Double> mapIDF = new HashMap<>();
        wordDocumentMap.forEach((word, documents) -> {
            double documentFrequency = documents.size();
            double idf = log10(documentsCount / documentFrequency);
            documents.forEach((documentName) -> {
                mapIDF.put(word + "@" + documentName, idf);
            });
        });

        FileWriter myWriter;
        try {
            myWriter = new FileWriter("Data/output.csv");
            myWriter.write("name,tf,idf,tfidf" + "\n");

            mapIDF.forEach((word, idf) -> {
                if (mapTF.containsKey(word)) {
                    double tf = mapTF.get(word);
                    double tfidf = idf * tf;
                    String out = word + "," + tf + "," + idf + "," + tfidf;
                    System.out.println(out);
                    try {
                        myWriter.write(out + "\n");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });

            myWriter.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void calculateDocumentCount(String docName) {
        if (!documents.contains(docName)) {
            if (overallWordCount.get("documentCount") == null) {
                documentsCount = 0d;
            } else {
                documentsCount = overallWordCount.get("documentCount");
            }
            documents.add(docName);
            documentsCount = (double) documents.size();
            overallWordCount.put("documentCount", documentsCount);
        }
    }
}
