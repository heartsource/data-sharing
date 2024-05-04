package com.heartsrc.loaders;

import org.apache.spark.api.java.JavaSparkContext;

public abstract class DataLoader {
    public abstract DataDefinition buildDataDefinition(JavaSparkContext sc);
}
