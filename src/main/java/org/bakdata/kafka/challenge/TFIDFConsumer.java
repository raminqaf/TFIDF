package org.bakdata.kafka.challenge;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class TFIDFConsumer {
    private final static String TOPIC = "streams-output";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(final String[] args) throws Exception {
        runConsumer();
    }

    static void runConsumer() throws InterruptedException, IOException {
        final Consumer<String, String> consumer = createConsumer();
        final int giveUp = 1000;
        int noRecordsCount = 0;
        FileWriter myWriter = new FileWriter("Data/output.csv");

        while (true) {
            Duration duration = Duration.ofMillis(1000L);
            final ConsumerRecords<String, String> consumerRecords = consumer.poll(duration);
            if (consumerRecords.count()==0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
                else continue;
            }
            consumerRecords.forEach(record -> {
                String out = String.format("%s, %s", record.key(), record.value()) + "\n";
                try {
                    myWriter.write(out);
                    myWriter.flush();

                } catch (IOException e) {
                    e.printStackTrace();
                }
                System.out.printf("Consumer Record:(%s, %s, %d, %d)\n",
                        record.key(), record.value(),
                        record.partition(), record.offset());
            });
            consumer.commitAsync();
        }
        consumer.close();
        myWriter.close();

        System.out.println("DONE");
    }

    private static Consumer<String, String> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // Create the consumer using props.
        final Consumer<String, String> consumer = new KafkaConsumer<>(props);
        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(TOPIC));
        return consumer;
    }
}
