package com.heartsrc.dto;

/**
 * This class defines how to link multiple data set together for
 * the sharing.  TODO: joining to be implemented
 */
public class LinkingDefinition {
    private String fromDatasetType = null;
    private String toDatasetType = null;

    /**
     * Something like
     *  {
     *      {"Age","age_group"},
     *      {"Gender","Male_Female"}
     *  }
     */
    private String[][] joins = null;

    public String getFromDatasetType() {
        return fromDatasetType;
    }

    public void setFromDatasetType(String fromDatasetType) {
        this.fromDatasetType = fromDatasetType;
    }

    public String getToDatasetType() {
        return toDatasetType;
    }

    public void setToDatasetType(String toDatasetType) {
        this.toDatasetType = toDatasetType;
    }

    public String[][] getJoins() {
        return joins;
    }

    public void setJoins(String[][] joins) {
        this.joins = joins;
    }
}
