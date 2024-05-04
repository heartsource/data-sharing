package com.heartsrc.utils;

import java.io.Serializable;
import java.util.Map;

/**
 * Provides a way for the data set to adjust the values as each row is process
 * - in spark uses a map function in which the adjusted row is returned
 */
public interface ValueAdjust extends Serializable {
    /**
     * Provides a way for the data set to adjust the values as each row is process
     * @param inRow data for the row
     * @param headers map where key is the header name and value is the index in the
     *                inRow in which the data for that column is found
     */
    public String[] adjustRowValues(String[] inRow, Map<String, Integer> headers);
}
