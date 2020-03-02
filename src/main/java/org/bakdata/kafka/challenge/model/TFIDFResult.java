package org.bakdata.kafka.challenge.model;

public class TFIDFResult {
    private String documentName;
    private double tfidf;
    private double overallDocumentCount;

    public TFIDFResult() { }

    public TFIDFResult(String documentName, double tfidf, double overallDocumentCount) {
        this.documentName = documentName;
        this.tfidf = tfidf;
        this.overallDocumentCount = overallDocumentCount;
    }

    public String getDocumentName() {
        return documentName;
    }

    public void setDocumentName(String documentName) {
        this.documentName = documentName;
    }

    public double getTfidf() {
        return tfidf;
    }

    public void setTfidf(double tfidf) {
        this.tfidf = tfidf;
    }

    public double getOverallDocumentCount() {
        return overallDocumentCount;
    }

    public void setOverallDocumentCount(double overallDocumentCount) {
        this.overallDocumentCount = overallDocumentCount;
    }

    @Override public String toString() {
        return "TFIDFResult{" +
                "documentName='" + documentName + '\'' +
                ", tfidf=" + tfidf +
                ", overallDocumentCount=" + overallDocumentCount +
                '}';
    }
}
