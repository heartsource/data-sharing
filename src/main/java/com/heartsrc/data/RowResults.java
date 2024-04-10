package com.heartsrc.data;

import java.io.Serializable;
import java.util.Arrays;

public abstract class RowResults implements Serializable {
    public String[] grouping = null;
    public String toString(){
        StringBuilder sb = new StringBuilder();
        return  Arrays.toString(grouping) + ": " + sb.toString();
    }
    public abstract Object[] getRowData();

}
