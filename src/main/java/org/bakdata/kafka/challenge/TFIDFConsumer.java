package org.bakdata.kafka.challenge;

import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.bakdata.kafka.challenge.customSerde.tfidfResultSerde.TFIDFResultDeserializer;
import org.bakdata.kafka.challenge.model.TFIDFResult;

public final class TFIDFConsumer {

    private TFIDFConsumer() {
    }

    public static void main(final String[] args) throws IOException {
        runConsumer();
    }

    private static void runConsumer() throws IOException {
        final Consumer<String, TFIDFResult> consumer = createConsumer();
        final int giveUp = 1000;
        int noRecordsCount = 0;
        final FileWriter myWriter = new FileWriter("Data/output.csv");

        while (true) {
            final Duration duration = Duration.ofMillis(1000L);
            final ConsumerRecords<String, TFIDFResult> consumerRecords = consumer.poll(duration);
            if (consumerRecords.count() == 0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) {
                    break;
                } else {
                    continue;
                }
            }
            consumerRecords.forEach(record -> {
                final TFIDFResult tfidfResult = record.value();
                final String out = String.format("%s, %s", record.key(), tfidfResult) + "\n";
                try {
                    myWriter.write(out);
                    myWriter.flush();

                } catch (final IOException e) {
                    e.printStackTrace();
                }
                System.out.printf("Consumer Record:(%s, %s)\n", record.key(), tfidfResult);
            });
            consumer.commitAsync();
        }
        consumer.close();
        myWriter.close();

        System.out.println("DONE");
    }

    private static Consumer<String, TFIDFResult> createConsumer() {
        final Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConstants.KAFKA_BOOTSTRAP_SERVERS);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, IKafkaConstants.CLIENT_ID_CONSUMER);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, TFIDFResultDeserializer.class.getName());
        // Create the consumer using props.
        final Consumer<String, TFIDFResult> consumer = new KafkaConsumer<>(props);
        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(IKafkaConstants.OUTPUT_TOPIC));
        return consumer;
    }
}
