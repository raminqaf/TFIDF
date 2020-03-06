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

package org.bakdata.kafka.challenge.tfidf;

import static org.bakdata.kafka.challenge.producer.TFIDFProducer.runProducer;

import com.bakdata.kafka.AbstractS3BackedConfig;
import com.bakdata.kafka.S3BackedSerde;
import com.bakdata.kafka.S3BackedSerdeConfig;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
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
import org.bakdata.kafka.challenge.constant.IKafkaConstants;
import org.bakdata.kafka.challenge.constant.IKeyValueStore;
import org.bakdata.kafka.challenge.customSerde.producerKeyInfoSerde.ProducerKeyInfoSerde;
import org.bakdata.kafka.challenge.customSerde.tfidfResultSerde.TFIDFResultSerde;
import org.bakdata.kafka.challenge.model.Information;
import org.bakdata.kafka.challenge.model.ProducerKeyInfo;
import org.bakdata.kafka.challenge.model.TFIDFResult;
import org.bakdata.kafka.challenge.s3.S3Handler;
import org.bakdata.kafka.challenge.transformer.TFIDFTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TFIDFTask implements Runnable {
    private static final String inputTopic = IKafkaConstants.INPUT_TOPIC;
    private static final String outputTopic = IKafkaConstants.OUTPUT_TOPIC;

    private static final Logger logger = LoggerFactory.getLogger(TFIDFTask.class);

    @Override
    public void run() {
        logger.info("TFIDF task started...");
        final Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", IKafkaConstants.KAFKA_BOOTSTRAP_SERVERS);
        final AdminClient admin = AdminClient.create(prop);
        final Set<String> topics;
        try {
            topics = admin.listTopics().names().get();

            logger.info("deleting the content of the input/output topics");
            admin.deleteTopics(topics);

            logger.info("creating the input topic");
            createTopic(admin, topics, inputTopic);
            logger.info("creating the output topic");
            createTopic(admin, topics, outputTopic);

            S3Handler.emptyS3Bucket(IKafkaConstants.S3_BUCKET_NAME, IKafkaConstants.S3_REGION);

            runProducer();
        } catch (final IOException | ExecutionException | InterruptedException e) {
            logger.error(e.getLocalizedMessage());
        }

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
        streamsConfiguration.setProperty(AbstractS3BackedConfig.S3_REGION_CONFIG, IKafkaConstants.S3_REGION_CONFIG);
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

        final KStream<String, Information> tf = textLines.flatMap(TFIDFTask::calculateTF);

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
        final Collection<KeyValue<String, Information>> wordInformationList = new ArrayList<>();
        wordFrequencyInDocument.forEach((word, count) -> {
            final double termFrequency = count / sumOfWordsInDocument;
            final Information information = new Information(termFrequency, documentName, documentCount);
            wordInformationList.add(new KeyValue<>(word, information));
        });
        return wordInformationList;
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
