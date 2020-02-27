package org.bakdata.kafka.challenge;

import com.bakdata.kafka.AbstractS3BackedConfig;
import com.bakdata.kafka.S3BackedSerde;
import com.bakdata.kafka.S3BackedSerdeConfig;
import com.bakdata.kafka.S3BackedSerializer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.omg.CORBA.DATA_CONVERSION;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class TFIDFProducer {
    private final static String TOPIC = "streams-plaintext-input";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";
    private final static String COMMA_DELIMITER = ",";
    private final static String DATA_PATH = "Data/vep_big_names_of_science_v2_txt/";

    static void runProducer() throws IOException {
        final Producer<String, String> producer = createProducer();
        try {
            List<File> files = getFilesToRead();

//            files = files.subList(0, 21);
//            files.removeAll(files);
//
//            files.add(new File(DATA_PATH + "document2.txt"));
//            files.add(new File(DATA_PATH + "document1.txt"));

            for (File file : files) {
                try {
                    List<String> listOfLines = Files.readAllLines(file.toPath());
                    String allLines = String.join("\n", listOfLines);
                    final ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, file.getName() + "-" + files.size(), allLines);
                    producer.send(record);
                    System.out.println("Sent: " + file.getName());
                } catch (NoSuchFileException e) {
                    System.out.println("This file doesn't exists: " + file.getName());
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            producer.flush();
            producer.close();
        }
    }

    private static Producer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "TFIDFProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, S3BackedSerializer.class.getName());
        props.setProperty(AbstractS3BackedConfig.BASE_PATH_CONFIG, "s3://bignamesofsience/");
        props.setProperty(AbstractS3BackedConfig.S3_REGION_CONFIG, "eu-central-1");
        props.put(S3BackedSerdeConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        return new KafkaProducer<>(props);
    }

    public static List<File> getFilesToRead() {
        List<File> files = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(DATA_PATH + "VEP_Big_Names_of_Science_Metadata.csv"))) {
            br.readLine();
            String line;
            while (br.ready()) {
                line = br.readLine();
                String textName = line.split(COMMA_DELIMITER)[1];
                String pathName = DATA_PATH + textName;
                File file = new File(pathName);
                if (file.exists()) {
                    files.add(file);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return files;
    }
}
