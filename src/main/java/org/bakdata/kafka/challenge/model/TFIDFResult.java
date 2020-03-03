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
