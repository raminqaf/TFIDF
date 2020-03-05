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

package org.bakdata.kafka.challenge.producer;

import com.bakdata.kafka.AbstractS3BackedConfig;
import com.bakdata.kafka.S3BackedSerdeConfig;
import com.bakdata.kafka.S3BackedSerializer;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.bakdata.kafka.challenge.constant.IKafkaConstants;
import org.bakdata.kafka.challenge.customSerde.producerKeyInfoSerde.ProducerKeyInfoSerializer;
import org.bakdata.kafka.challenge.model.ProducerKeyInfo;

public final class TFIDFProducer {
    private static final String COMMA_DELIMITER = ",";
    private static final String DATA_PATH = "Data/vep_big_names_of_science_v2_txt/";

    private TFIDFProducer() {
    }

    public static void runProducer() throws IOException {
        final Producer<ProducerKeyInfo, String> producer = createProducer();
        try {
            final List<File> files = getFilesToRead();

//            files = files.subList(0, 2);
//            files.removeAll(files);
//
//            files.add(new File(DATA_PATH + "document1.txt"));
//            files.add(new File(DATA_PATH + "document2.txt"));

            int filesCount = 0;

            for (final File file : files) {
                try {
                    filesCount++;
                    final List<String> listOfLines = Files.readAllLines(file.toPath());
                    final String allLines = String.join("\n", listOfLines);
                    final ProducerKeyInfo producerKeyInfo = new ProducerKeyInfo(file.getName(), filesCount);
                    final ProducerRecord<ProducerKeyInfo, String> record =
                            new ProducerRecord<>(IKafkaConstants.INPUT_TOPIC, producerKeyInfo, allLines);
                    producer.send(record);
                    System.out.println("Sent: " + file.getName());
                } catch (final NoSuchFileException e) {
                    System.out.println("This file doesn't exists: " + file.getName());
                }
            }

        } catch (final IOException e) {
            e.printStackTrace();
        } finally {
            producer.flush();
            producer.close();
        }
    }

    private static Producer<ProducerKeyInfo, String> createProducer() {
        final Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConstants.KAFKA_BOOTSTRAP_SERVERS);
        props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, IKafkaConstants.CLIENT_ID_PRODUCER);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ProducerKeyInfoSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, S3BackedSerializer.class.getName());
        props.setProperty(AbstractS3BackedConfig.BASE_PATH_CONFIG, IKafkaConstants.S3_BASE_PATH);
        props.setProperty(AbstractS3BackedConfig.S3_REGION_CONFIG, IKafkaConstants.S3_REGION);
        props.put(S3BackedSerdeConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        return new KafkaProducer<>(props);
    }

    public static List<File> getFilesToRead() {
        final List<File> files = new ArrayList<>();
        try (final BufferedReader br = new BufferedReader(
                new FileReader(DATA_PATH + "VEP_Big_Names_of_Science_Metadata.csv"))) {
            br.readLine();
            String line;
            while (br.ready()) {
                line = br.readLine();
                final String textName = line.split(COMMA_DELIMITER)[1];
                final String pathName = DATA_PATH + textName;
                final File file = new File(pathName);
                if (file.exists()) {
                    files.add(file);
                }
            }
        } catch (final IOException e) {
            e.printStackTrace();
        }
        return files;
    }
}
