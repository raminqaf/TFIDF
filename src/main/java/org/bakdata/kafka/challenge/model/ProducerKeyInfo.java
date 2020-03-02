package org.bakdata.kafka.challenge.model;

public class ProducerKeyInfo {
    private String documentName;
    private double documentNumber;

    public ProducerKeyInfo() { }

    public ProducerKeyInfo(String documentName, double documentNumber) {
        this.documentName = documentName;
        this.documentNumber = documentNumber;
    }

    public String getDocumentName() {
        return documentName;
    }

    public void setDocumentName(String documentName) {
        this.documentName = documentName;
    }

    public double getDocumentNumber() {
        return documentNumber;
    }

    public void setDocumentNumber(double documentNumber) {
        this.documentNumber = documentNumber;
    }
}
