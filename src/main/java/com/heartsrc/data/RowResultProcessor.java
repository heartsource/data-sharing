package com.heartsrc.data;

import java.io.Serializable;

public interface RowResultProcessor extends Serializable {
    public void process(Object[] row) throws Exception;
}
