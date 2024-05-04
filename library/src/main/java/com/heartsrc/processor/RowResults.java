package com.heartsrc.processor;

import java.io.Serializable;

public abstract class RowResults implements Serializable {
    public String[] grouping = null;

    public abstract Object[] getRowData();

}
