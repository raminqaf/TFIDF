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

package org.bakdata.kafka.challenge.consumer;

import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.bakdata.kafka.challenge.constant.IDirectoryConstants;
import org.bakdata.kafka.challenge.constant.IKafkaConstants;
import org.bakdata.kafka.challenge.customSerde.tfidfResultSerde.TFIDFResultDeserializer;
import org.bakdata.kafka.challenge.model.TFIDFResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerTask implements Runnable {
    private final long timeOutInSeconds;

    private static final Logger logger = LoggerFactory.getLogger(ConsumerTask.class);
    private static final String PATH_TO_OUTPUT_FILE =
            IDirectoryConstants.DATA_DIRECTORY + IDirectoryConstants.OUTPUT_FILE_NAME;

    public ConsumerTask(final long timeOutInSeconds) {
        this.timeOutInSeconds = timeOutInSeconds;
    }

    @Override
    public void run() {
        logger.info(String.format("Consumer with a timeout of %d seconds started", this.timeOutInSeconds));
        final Consumer<String, TFIDFResult> consumer = createConsumer();
        final FileWriter myWriter;
        final LocalDateTime then = LocalDateTime.now();
        try {
            myWriter = new FileWriter(PATH_TO_OUTPUT_FILE);
            while (ChronoUnit.SECONDS.between(then, LocalDateTime.now()) < this.timeOutInSeconds) {
                final Duration duration = Duration.ofMillis(1000L);
                final ConsumerRecords<String, TFIDFResult> consumerRecords = consumer.poll(duration);
                consumerRecords.forEach(record -> {
                    final TFIDFResult tfidfResult = record.value();
                    final String out = String.format("%s, %s", record.key(), tfidfResult) + "\n";
                    try {
                        myWriter.write(out);
                        myWriter.flush();

                    } catch (final IOException e) {
                        logger.error(e.getLocalizedMessage());
                    }
                    logger.info(String.format("Consumer Record:(%s, %s)", record.key(), tfidfResult));
                });
                consumer.commitAsync();
            }

            consumer.close();
            myWriter.close();
            logger.info("Consumer closed");

        } catch (final IOException e) {
            logger.error(e.getLocalizedMessage());
        }
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
