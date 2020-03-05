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

public class TFIDFResult {
    private String documentName = "";
    private double tfidf = 0.0;
    private double overallDocumentCount = 0.0;

    public TFIDFResult() {
    }

    public TFIDFResult(final String documentName, final double tfidf, final double overallDocumentCount) {
        this.documentName = documentName;
        this.tfidf = tfidf;
        this.overallDocumentCount = overallDocumentCount;
    }

    public String getDocumentName() {
        return this.documentName;
    }

    public void setDocumentName(final String documentName) {
        this.documentName = documentName;
    }

    public double getTfidf() {
        return this.tfidf;
    }

    public void setTfidf(final double tfidf) {
        this.tfidf = tfidf;
    }

    public double getOverallDocumentCount() {
        return this.overallDocumentCount;
    }

    public void setOverallDocumentCount(final double overallDocumentCount) {
        this.overallDocumentCount = overallDocumentCount;
    }

    @Override
    public String toString() {
        return "TFIDFResult{" +
                "documentName='" + this.documentName + '\'' +
                ", tfidf=" + this.tfidf +
                ", overallDocumentCount=" + this.overallDocumentCount +
                '}';
    }
}
