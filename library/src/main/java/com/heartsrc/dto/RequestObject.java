package com.heartsrc.dto;

/**
 * Defines the definition of the data to be produced to be shared
 */
public class RequestObject {
    DatasetDefinition[] datasets = null;
    LinkingDefinition[] linking = null;
    public DatasetDefinition[] getDatasets() {
        return datasets;
    }

    public void setDatasets(DatasetDefinition[] datasets) {
        this.datasets = datasets;
    }

    public LinkingDefinition[] getLinking() {
        return linking;
    }

    public void setLinking(LinkingDefinition[] linking) {
        this.linking = linking;
    }
}
