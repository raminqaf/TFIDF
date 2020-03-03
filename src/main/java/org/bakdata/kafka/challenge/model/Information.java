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
