package com.heartsrc.loaders;

import org.apache.spark.api.java.JavaRDD;

import java.util.Map;

/**
 * This class holds the RDD of row data  and the metadata about the columns
 * for each row
 */
public class DataDefinition {
    private Map<String, Integer> columnHeaders = null;
    private  JavaRDD<String[]>  initialRDD=null;

    /**
     * Map of column name as the key and index of that column in the row of data
     */
    public Map<String, Integer> getColumnHeaders() {
        return columnHeaders;
    }

    /**
     * Map of column name as the key and index of that column in the row of data
     */
    public void setColumnHeaders(Map<String, Integer> columnHeaders) {
        this.columnHeaders = columnHeaders;
    }

    /**
     * The RDD of String[] (rows) for processing using spark
     */
    public JavaRDD<String[]> getInitialRDD() {
        return initialRDD;
    }

    /**
     * The RDD of String[] (rows) for processing using spark
     */
    public void setInitialRDD(JavaRDD<String[]> initialRDD) {
        this.initialRDD = initialRDD;
    }
}
