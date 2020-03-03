package org.bakdata.kafka.challenge;

import static java.lang.Math.log10;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public final class TFIDFCalculator {
    private static final String DATA_PATH = "Data/vep_big_names_of_science_v2_txt/";

    private TFIDFCalculator() {
    }

    public static void main(final String[] args) throws Exception {
        final List<File> files = TFIDFProducer.getFilesToRead();

//        files = files.subList(0, 21);
//        files.removeAll(files);
//
//        files.add(new File(DATA_PATH + "document2.txt"));
//        files.add(new File(DATA_PATH + "document1.txt"));

        final long documentCount = files.size();
        final Map<String, String> mapTF = new HashMap<>();
        final Map<String, Set<String>> wordDocument = new HashMap<>();
        final FileWriter myWriter = new FileWriter("Data/output-calc.csv");
        myWriter.write("name,tf,idf,tfidf" + "\n");

        files.forEach((file -> {
            final List<String> listOfLines;
            try {
                listOfLines = Files.readAllLines(file.toPath());
                final List<String> listOfLinesLowerCase =
                        listOfLines.stream().map(String::toLowerCase).collect(Collectors.toList());
                final String fileContent = String.join("\n", listOfLinesLowerCase);
                calculateTF(file.getName(), fileContent, mapTF);
                storeAllWords(file.getName(), fileContent, wordDocument);
            } catch (final IOException e) {
                e.printStackTrace();
            }
        }));

        final Map<String, String> mapIDF = new HashMap<>();
        wordDocument.forEach((word, documentOccurrence) -> {
            final double documentFrequency = documentOccurrence.size();
            final double idf = log10(documentCount / documentFrequency);
            documentOccurrence.forEach((documentName) -> {
                mapIDF.put(word + "@" + documentName, String.valueOf(idf));
            });
        });

        mapIDF.forEach((word, idf) -> {
            if (mapTF.containsKey(word)) {
                final double tf = Double.parseDouble(mapTF.get(word));
                final double tfidf = Double.parseDouble(idf) * tf;
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
        System.out.println("DONE");
    }

    private static void calculateTF(final String fileName, final String fileContent, final Map<String, String> mapTF) {

        final Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);
        final List<String> listOfWords = Arrays.asList(pattern.split(fileContent.toLowerCase()));
        final Map<String, Long> wordFreq = new HashMap<>();

        listOfWords.forEach(word -> {
            if (!wordFreq.containsKey(word)) {
                wordFreq.put(word, 1L);
            } else {
                long count = wordFreq.get(word);
                count++;
                wordFreq.put(word, count);
            }
        });
        final double sumOfWordsInDocument = listOfWords.size();

        wordFreq.forEach((word, count) -> {
            final double termFrequency = count / sumOfWordsInDocument;
            mapTF.put(word + "@" + fileName, String.valueOf(termFrequency));
        });
    }

    private static void storeAllWords(final String documentName, final String fileContent,
            final Map<String, Set<String>> wordDocumentMap) {
        final Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);
        final List<String> listOfWords = Arrays.asList(pattern.split(fileContent));
        listOfWords.forEach((word) -> {
            final Set<String> setMap = wordDocumentMap.get(word);
            if (setMap != null) {
                setMap.add(documentName);
                wordDocumentMap.put(word, setMap);
            } else {
                final Set<String> set = new HashSet<>();
                set.add(documentName);
                wordDocumentMap.put(word, set);
            }
        });
    }
}
