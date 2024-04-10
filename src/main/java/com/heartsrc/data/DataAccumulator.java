package com.heartsrc.data;

import java.io.Serializable;

public interface DataAccumulator extends Serializable {
    public DataAccumulator adjustValues(DataAccumulator other);


}
