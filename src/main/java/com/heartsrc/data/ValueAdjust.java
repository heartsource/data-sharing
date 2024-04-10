package com.heartsrc.data;

import java.io.Serializable;
import java.util.Map;

public interface ValueAdjust extends Serializable {
    public String[] adjustRowValues(String[] inRow, Map<String, Integer> headers);
}
