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

package org.bakdata.kafka.challenge.model;

public class Information {
    private double TermFrequency = 0.0;
    private String DocumentName = "";
    private double OverallDocumentCount = 0.0;

    public Information() {
    }

    public Information(final double termFrequency, final String documentName, final double overallDocumentCount) {
        this.TermFrequency = termFrequency;
        this.DocumentName = documentName;
        this.OverallDocumentCount = overallDocumentCount;
    }

    public double getTermFrequency() {
        return this.TermFrequency;
    }

    public void setTermFrequency(final double termFrequency) {
        this.TermFrequency = termFrequency;
    }

    public String getDocumentName() {
        return this.DocumentName;
    }

    public void setDocumentName(final String documentName) {
        this.DocumentName = documentName;
    }

    public double getOverallDocumentCount() {
        return this.OverallDocumentCount;
    }

    public void setOverallDocumentCount(final double overallDocumentCount) {
        this.OverallDocumentCount = overallDocumentCount;
    }
}
