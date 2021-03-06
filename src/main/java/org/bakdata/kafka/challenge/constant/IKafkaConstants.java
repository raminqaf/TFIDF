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

package org.bakdata.kafka.challenge.constant;

public interface IKafkaConstants {
    String APPLICATION_ID_CONFIG = "TFIDF";
    String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    String INPUT_TOPIC = "streams-plaintext-input";
    String OUTPUT_TOPIC = "streams-output";
    String S3_BUCKET_NAME = "bignamesofsience";
    String S3_BASE_PATH = "s3://" + S3_BUCKET_NAME + "/";
    String S3_REGION = "eu-central-1";
    String S3_REGION_CONFIG = "s3.eu-central-1";
    String CLIENT_ID_PRODUCER = "TFIDFProducer";
    String CLIENT_ID_CONSUMER = "TFIDFConsumer";
}
