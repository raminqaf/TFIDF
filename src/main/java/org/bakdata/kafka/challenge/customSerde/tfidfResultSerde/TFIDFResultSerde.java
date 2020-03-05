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

package org.bakdata.kafka.challenge.customSerde.tfidfResultSerde;

import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.bakdata.kafka.challenge.model.TFIDFResult;

public class TFIDFResultSerde implements Serde<TFIDFResult> {

    private final Serde<TFIDFResult> inner;

    public TFIDFResultSerde() {
        this.inner = Serdes.serdeFrom(new TFIDFResultSerializer(), new TFIDFResultDeserializer());
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        this.inner.configure(configs, isKey);
    }

    @Override
    public void close() {
        this.inner.close();
    }

    @Override
    public Serializer<TFIDFResult> serializer() {
        return this.inner.serializer();
    }

    @Override
    public Deserializer<TFIDFResult> deserializer() {
        return this.inner.deserializer();
    }
}
