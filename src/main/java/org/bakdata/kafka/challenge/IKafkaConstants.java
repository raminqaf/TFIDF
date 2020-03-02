package org.bakdata.kafka.challenge;

public interface IKafkaConstants {
    String APPLICATION_ID_CONFIG = "TFIDF";
    String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    String INPUT_TOPIC = "streams-plaintext-input";
    String OUTPUT_TOPIC = "streams-output";
    String S3_BASE_PATH = "s3://bignamesofsience/";
    String S3_REGION = "s3.eu-central-1";
    String CLIENT_ID_PRODUCER = "TFIDFProducer";
    String CLIENT_ID_CONSUMER = "TFIDFConsumer";
}
