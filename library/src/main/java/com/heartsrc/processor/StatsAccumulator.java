package com.heartsrc.processor;

import java.io.Serializable;

public interface StatsAccumulator extends Serializable {
    public StatsAccumulator adjustValues(StatsAccumulator other);
    public void processResult(RowResults res);


}
