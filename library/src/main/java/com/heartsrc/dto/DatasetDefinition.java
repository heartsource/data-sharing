package com.heartsrc.dto;

import java.util.Map;

/**
 * Defines the data set to be used as inputs for the data sharing.  User
 * within the Requested to process in the input data into sharable data.
 */
public class DatasetDefinition {

    private String type = null;
    private String[] keys = null;
    private String[] measures = null;
    private String[] groupByFields = null;

    /**
     * Something like "Age" - ["in", "18-24","25-34","35-44"]
     * Start without ORs making everything ADDs until otherwise needed
     */
    private Map<String, String[]> filters = null;

    /**
     * String used to identify the dataset type or tin other words the raw
     * data provider
     */
    public String getType() {
        return type;
    }

    /**
     * String used to identify the dataset type or tin other words the raw
     * data provider
     */
    public void setType(String type) {
        this.type = type;
    }

    /**
     * When the datasets from the data provides are incrementally provided the
     * key is used to be the increment that is provided.  It might be a file name
     * of the date to identify the batch of data, or it might be a value in a
     * column of a DB table to date the grouping by the delivery of data that can
     * be processes
     */
    public String[] getKeys() {
        return keys;
    }

    /**
     * When the datasets from the data provides are incrementally provided the
     * key is used to be the increment that is provided.  It might be a file name
     * of the date to identify the batch of data, or it might be a value in a
     * column of a DB table to date the grouping by the delivery of data that can
     * be processes
     */
    public void setKeys(String[] keys) {
        this.keys = keys;
    }

    /**
     * Some datasets might have business rules on producing measurement, classifications,
     * statistics for the grouped filed.  This variable is the name of those business rules
     * that are to be included in the results of the data shared.
     */
    public String[] getMeasures() {
        return measures;
    }

    /**
     * Some datasets might have business rules on producing measurement, classifications,
     * statistics for the grouped filed.  This variable is the name of those business rules
     * that are to be included in the results of the data shared.
     */
    public void setMeasures(String[] measures) {
        this.measures = measures;
    }

    /**
     * The fields in the source data sets that the caller wants the measures to be produced for
     */
    public String[] getGroupByFields() {
        return groupByFields;
    }

    /**
     * The fields in the source data sets that the caller wants the measures to be produced for
     */
    public void setGroupByFields(String[] groupByFields) {
        this.groupByFields = groupByFields;
    }

    /**
     * A filter to apply to the raw dataset source data.  Key will be the column name
     * value will be a String[] with the first element describing the type of evaluation
     * and the remaining elements build the value(s).  If the first element is not one of the
     * evaluation types supported or only one element, it should default to "equals" when one
     * element or "in" with multiple elements.  TODO: filtering yet to be implemented
     */
    public Map<String, String[]> getFilters() {
        return filters;
    }

    /**
     * A filter to apply to the raw dataset source data.  Key will be the column name
     * value will be a String[] with the first element describing the type of evaluation
     * and the remaining elements build the value(s).  If the first element is not one of the
     * evaluation types supported or only one element, it should default to "equals" when one
     * element or "in" with multiple elements
     */
    public void setFilters(Map<String, String[]> filters) {
        this.filters = filters;
    }
}
