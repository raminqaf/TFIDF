package org.bakdata.kafka.challenge.model;

public class ProducerKeyInfo {
    private String documentName = "";
    private double documentNumber = 0.0;

    public ProducerKeyInfo() {
    }

    public ProducerKeyInfo(final String documentName, final double documentNumber) {
        this.documentName = documentName;
        this.documentNumber = documentNumber;
    }

    public String getDocumentName() {
        return this.documentName;
    }

    public void setDocumentName(final String documentName) {
        this.documentName = documentName;
    }

    public double getDocumentNumber() {
        return this.documentNumber;
    }

    public void setDocumentNumber(final double documentNumber) {
        this.documentNumber = documentNumber;
    }
}
