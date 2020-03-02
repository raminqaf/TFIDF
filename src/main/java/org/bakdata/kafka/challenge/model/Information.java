package org.bakdata.kafka.challenge.model;

public class Information {
    private double TermFrequency;
    private String DocumentName;
    private double OverallDocumentCount;

    public Information() { }

    public Information(double termFrequency, String documentName, double overallDocumentCount) {
        TermFrequency = termFrequency;
        DocumentName = documentName;
        OverallDocumentCount = overallDocumentCount;
    }

    public double getTermFrequency() {
        return TermFrequency;
    }

    public void setTermFrequency(double termFrequency) {
        TermFrequency = termFrequency;
    }

    public String getDocumentName() {
        return DocumentName;
    }

    public void setDocumentName(String documentName) {
        DocumentName = documentName;
    }

    public double getOverallDocumentCount() {
        return OverallDocumentCount;
    }

    public void setOverallDocumentCount(double overallDocumentCount) {
        OverallDocumentCount = overallDocumentCount;
    }
}
