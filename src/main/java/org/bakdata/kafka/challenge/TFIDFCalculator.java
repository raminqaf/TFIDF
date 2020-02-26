package org.bakdata.kafka.challenge;

import org.apache.kafka.streams.KeyValue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static java.lang.Math.log10;

public class TFIDFCalculator {
    private final static String DATA_PATH = "Data/vep_big_names_of_science_v2_txt/";

    public static void main(final String[] args) throws Exception {
        List<File> files = TFIDFProducer.getFilesToRead();

        files = files.subList(0, 21);
//        files.removeAll(files);
//
//        files.add(new File(DATA_PATH + "document2.txt"));
//        files.add(new File(DATA_PATH + "document1.txt"));


        long documentCount = files.size();
        Map<String, String> mapTF = new HashMap<>();
        Map<String, Set<String>> wordDocument = new HashMap<>();
        FileWriter myWriter = new FileWriter("Data/output-calc.csv");
        myWriter.write("name,tf,idf,tfidf" + "\n");

        files.forEach((file -> {
            List<String> listOfLines;
            try {
                listOfLines = Files.readAllLines(file.toPath());
                String fileContent = String.join("\n", listOfLines);
                calculateTF(file.getName(), fileContent, mapTF);
                storeAllWords(file.getName(), fileContent, wordDocument);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }));

        Map<String, String> mapIDF = new HashMap<>();
        wordDocument.forEach((word, documentOccurrence) -> {
            double documentFrequency = documentOccurrence.size();
            double idf = log10(documentCount/documentFrequency);
            documentOccurrence.forEach((documentName) -> {
                mapIDF.put(word + "@" + documentName, String.valueOf(idf));
            });
        });

        mapIDF.forEach((word, idf) -> {
            if(mapTF.containsKey(word)) {
                double tf = Double.parseDouble(mapTF.get(word));
                double tfidf = Double.parseDouble(idf) * tf;
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
        System.out.println("DONE");
    }

    private static void calculateTF(String fileName, String fileContent, Map<String, String> listTF) {
        final Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);
        List<String> listOfWords = Arrays.asList(pattern.split(fileContent.toLowerCase()));
        Map<String, Long> wordFreq = new HashMap<>();

        listOfWords.forEach(word -> {
            if (!wordFreq.containsKey(word)) {
                wordFreq.put(word, 1L);
            } else {
                long count = wordFreq.get(word);
                count++;
                wordFreq.put(word, count);
            }
        });
        double sumOfWordsInDocument = listOfWords.size();
        wordFreq.forEach((word, count) -> {
            double termFrequency = count / sumOfWordsInDocument;
            listTF.put(word + "@" + fileName, String.valueOf(termFrequency));
        });
    }

    private static void storeAllWords(String documentName, String fileContent, Map<String, Set<String>> map) {
        final Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);
        List<String> listOfWords = Arrays.asList(pattern.split(fileContent));
        listOfWords.forEach((word) -> {
            Set<String> setMap = map.get(word);
            if (setMap != null) {
                setMap.add(documentName);
                map.put(word, setMap);
            } else {
                Set<String> set = new HashSet<>();
                set.add(documentName);
                map.put(word, set);
            }
        });
    }
}
