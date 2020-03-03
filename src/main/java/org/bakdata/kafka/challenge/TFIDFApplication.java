package org.bakdata.kafka.challenge;

import static org.bakdata.kafka.challenge.TFIDFProducer.runProducer;

import com.bakdata.kafka.AbstractS3BackedConfig;
import com.bakdata.kafka.S3BackedSerde;
import com.bakdata.kafka.S3BackedSerdeConfig;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.bakdata.kafka.challenge.customSerde.producerKeyInfoSerde.ProducerKeyInfoSerde;
import org.bakdata.kafka.challenge.customSerde.tfidfResultSerde.TFIDFResultSerde;
import org.bakdata.kafka.challenge.model.Information;
import org.bakdata.kafka.challenge.model.ProducerKeyInfo;
import org.bakdata.kafka.challenge.model.TFIDFResult;

public final class TFIDFApplication {
    static final String inputTopic = IKafkaConstants.INPUT_TOPIC;
    static final String outputTopic = IKafkaConstants.OUTPUT_TOPIC;

    private TFIDFApplication() {
    }

    public static void main(final String[] args) throws Exception {
        final Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "localhost:9092");
        final AdminClient admin = AdminClient.create(prop);
        final Set<String> topics = admin.listTopics().names().get();
        admin.deleteTopics(topics);
        createTopic(admin, topics, inputTopic);
        createTopic(admin, topics, outputTopic);
        runProducer();

        // Configure the Streams application.
        final Properties streamsConfiguration = getStreamsConfiguration();

        // Define the processing topology of the Streams application.
        final StreamsBuilder builder = new StreamsBuilder();
        createWordCountStream(builder);
        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static void createTopic(final Admin admin, final Collection<String> topics, final String topic) {
        if (topics.stream().noneMatch(topicName -> topicName.equalsIgnoreCase(topic))) {
            final Collection<NewTopic> topicList = new ArrayList<>();
            final Map<String, String> configs = new HashMap<>();
            final int partitions = 1;
            final short replication = 1;
            final NewTopic newTopic = new NewTopic(topic, partitions, replication).configs(configs);
            topicList.add(newTopic);
            admin.createTopics(topicList);
        }
    }

    static Properties getStreamsConfiguration() {
        final Properties streamsConfiguration = new Properties();

        streamsConfiguration.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, IKafkaConstants.APPLICATION_ID_CONFIG);
        streamsConfiguration
                .setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConstants.KAFKA_BOOTSTRAP_SERVERS);

        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, ProducerKeyInfoSerde.class);
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, S3BackedSerde.class);
        streamsConfiguration.setProperty(AbstractS3BackedConfig.BASE_PATH_CONFIG, IKafkaConstants.S3_BASE_PATH);
        streamsConfiguration.setProperty(AbstractS3BackedConfig.S3_REGION_CONFIG, IKafkaConstants.S3_REGION);
        streamsConfiguration.put(S3BackedSerdeConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        return streamsConfiguration;
    }

    static void createWordCountStream(final StreamsBuilder builder) {
        final KStream<ProducerKeyInfo, String> textLines = builder.stream(inputTopic);

        // create store
        final StoreBuilder<KeyValueStore<String, Double>> keyValueStoreBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore(IKeyValueStore.PERSISTENT_KV_OVERALL_WORD_COUNT),
                        Serdes.String(),
                        Serdes.Double());

        // register store
        builder.addStateStore(keyValueStoreBuilder);

        final KStream<String, Information> tf = textLines.flatMap(TFIDFApplication::calculateTF);

        final KStream<String, TFIDFResult> kv =
                tf.transform(TFIDFTransformer::new, IKeyValueStore.PERSISTENT_KV_OVERALL_WORD_COUNT);

        kv.to(outputTopic, Produced.with(Serdes.String(), new TFIDFResultSerde()));

    }

    private static Collection<KeyValue<String, Information>> calculateTF(final ProducerKeyInfo documentNameAndCount,
            final String fileContent) {
        final Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);
        final String documentName = documentNameAndCount.getDocumentName();
        final double documentCount = documentNameAndCount.getDocumentNumber();
        final List<String> listOfWords = Arrays.asList(pattern.split(fileContent.toLowerCase()));
        final Map<String, Long> wordFrequencyInDocument = new HashMap<>();
        listOfWords.forEach(word -> storeWordFrequency(wordFrequencyInDocument, word));
        final double sumOfWordsInDocument = listOfWords.size();
        final Collection<KeyValue<String, Information>> list = new ArrayList<>();
        wordFrequencyInDocument.forEach((word, count) -> {
            final double termFrequency = count / sumOfWordsInDocument;
            final Information information = new Information(termFrequency, documentName, documentCount);
            list.add(new KeyValue<>(word, information));
        });
        return list;
    }

    private static void storeWordFrequency(final Map<? super String, Long> wordFrequencyInDocument, final String word) {
        if (wordFrequencyInDocument.containsKey(word)) {
            long count = wordFrequencyInDocument.get(word);
            count++;
            wordFrequencyInDocument.put(word, count);
        } else {
            wordFrequencyInDocument.put(word, 1L);
        }
    }
}

