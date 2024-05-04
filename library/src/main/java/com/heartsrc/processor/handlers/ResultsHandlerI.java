package com.heartsrc.processor.handlers;

import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;

public interface ResultsHandlerI extends Serializable {
    public void handle(JavaRDD<Object[]> resultRDD, String[] resultHeaders) throws Exception;
}
