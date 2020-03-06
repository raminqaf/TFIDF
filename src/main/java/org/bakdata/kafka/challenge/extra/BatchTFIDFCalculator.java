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

package org.bakdata.kafka.challenge.extra;

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
import org.bakdata.kafka.challenge.constant.IDirectoryConstants;
import org.bakdata.kafka.challenge.producer.TFIDFProducer;

public final class BatchTFIDFCalculator {
    private static final String OUTPUT_PATH =
            IDirectoryConstants.DATA_DIRECTORY + IDirectoryConstants.OUTPUT_FILE_NAME_CALC;

    private BatchTFIDFCalculator() {
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
        final FileWriter fileWriter = new FileWriter(OUTPUT_PATH);
        fileWriter.write("name,tf,idf,tfidf" + "\n");

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
                    fileWriter.write(out + "\n");
                } catch (final IOException e) {
                    e.printStackTrace();
                }
            }
        });
        fileWriter.close();
        System.out.println("DONE");
    }

    private static void calculateTF(final String fileName, final String fileContent, final Map<String, String> mapTF) {

        final Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);
        final List<String> listOfWords = Arrays.asList(pattern.split(fileContent.toLowerCase()));
        final Map<String, Long> wordFreq = new HashMap<>();

        listOfWords.forEach(word -> {
            if (wordFreq.containsKey(word)) {
                long count = wordFreq.get(word);
                count++;
                wordFreq.put(word, count);
            } else {
                wordFreq.put(word, 1L);
            }
        });
        final double sumOfWordsInDocument = listOfWords.size();

        wordFreq.forEach((word, count) -> {
            final double termFrequency = count / sumOfWordsInDocument;
            mapTF.put(word + "@" + fileName, String.valueOf(termFrequency));
        });
    }

    private static void storeAllWords(final String documentName, final String fileContent,
            final Map<? super String, Set<String>> wordDocumentMap) {
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
