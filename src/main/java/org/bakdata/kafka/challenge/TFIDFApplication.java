package org.bakdata.kafka.challenge;

import com.bakdata.kafka.AbstractS3BackedConfig;
import com.bakdata.kafka.S3BackedSerde;
import com.bakdata.kafka.S3BackedSerdeConfig;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import static java.lang.Math.log10;
import static org.bakdata.kafka.challenge.TFIDFProducer.runProducer;

public class TFIDFApplication {
    static final String inputTopic = "streams-plaintext-input";
    static final String outputTopic = "streams-output";
    static long documentCount = 0L;

    public static void main(final String[] args) throws Exception {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "localhost:9092");
        AdminClient admin = AdminClient.create(prop);
        Set<String> topics = admin.listTopics().names().get();
        admin.deleteTopics(topics);
        createTopic(admin, topics, inputTopic);
        createTopic(admin, topics, outputTopic);
        runProducer();

        // Configure the Streams application.
        final Properties streamsConfiguration = getStreamsConfiguration(bootstrapServers);

        // Define the processing topology of the Streams application.
        final StreamsBuilder builder = new StreamsBuilder();
        createWordCountStream(builder);
        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static void createTopic(AdminClient admin, Set<String> topics, String topic) {
        if (topics.stream().noneMatch(topicName -> topicName.equalsIgnoreCase(topic))) {
            List<NewTopic> topicList = new ArrayList<>();
            Map<String, String> configs = new HashMap<>();
            int partitions = 1;
            short replication = 1;
            NewTopic newTopic = new NewTopic(topic, partitions, replication).configs(configs);
            topicList.add(newTopic);
            admin.createTopics(topicList);
        }
    }

    static Properties getStreamsConfiguration(final String bootstrapServers) {
        final Properties streamsConfiguration = new Properties();

        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "TFIDF");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, S3BackedSerde.class);
        streamsConfiguration.setProperty(AbstractS3BackedConfig.BASE_PATH_CONFIG, "s3://bignamesofsience/");
        streamsConfiguration.setProperty(AbstractS3BackedConfig.S3_REGION_CONFIG, "s3.eu-central-1");
        streamsConfiguration.put(S3BackedSerdeConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        return streamsConfiguration;
    }

    static void createWordCountStream(final StreamsBuilder builder) {
        final KStream<String, String> textLines = builder.stream(inputTopic);

        final Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);

        Map<String, Set<String>> map = new HashMap<>();

        KStream<String, String> tf =
                textLines.flatMap((documentNameAndCount, fileContent) -> {
                    String documentName = documentNameAndCount.split("-")[0];
                    documentCount = Long.parseLong(documentNameAndCount.split("-")[1]);
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
                    List<KeyValue<String, String>> list = new ArrayList<>();
                    wordFreq.forEach((word, count) -> {
                        double termFrequency = count / sumOfWordsInDocument;
                        list.add(new KeyValue<>(word, termFrequency + "@" + documentName));
                    });
                    return list;
                });

        final KTable<String, Double> idf = textLines
                .flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase())))
                .map((key, word) -> new KeyValue<>(word, key))
                .groupByKey()
                .aggregate(
                        () -> 0d,
                        (aggKey, newValue, aggValue) -> {
                            double documentF;
                            Set<String> setMap = map.get(aggKey);
                            if (setMap != null) {
                                setMap.add(newValue);
                                map.put(aggKey, setMap);
                                documentF = setMap.size();
                            } else {
                                Set<String> set = new HashSet<>();
                                set.add(newValue);
                                map.put(aggKey, set);
                                documentF = set.size();
                            }
                            aggValue = log10(documentCount / documentF);
                            return aggValue;
                        },
                        Materialized.<String, Double, KeyValueStore<Bytes, byte[]>>as("aggregated-table-store" /* state store name */)
                                .withKeySerde(Serdes.String()) /* key serde */
                                .withValueSerde(Serdes.Double())
                );

        tf.join(idf, (tfValue, idfValue) -> {
            double tfCount = Double.parseDouble(tfValue.split("@")[0]);
            String documentName = tfValue.split("@")[1];
            return (tfCount * idfValue) + "@" + documentName;
        }).map((key, value) -> {
            String tfidf = value.split("@")[0];
            String documentName = value.split("@")[1];
            return new KeyValue<>(String.format("tfidf(%s, %s, D)", key, documentName), tfidf);
        }).to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));
    }
}

