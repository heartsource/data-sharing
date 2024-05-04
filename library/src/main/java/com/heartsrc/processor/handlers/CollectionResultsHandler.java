package com.heartsrc.processor.handlers;

import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayList;
import java.util.List;

public class CollectionResultsHandler implements ResultsHandlerI {
    private List<Object[]> results = null;

    @Override
    public void handle(JavaRDD<Object[]> resultRDD, String[] resultHeaders) throws Exception{
        results = new ArrayList<>();
        results.add(resultHeaders);
        List<Object[]> rowList = resultRDD.collect();
        results.addAll(rowList);
    }

    public List<Object[]> getResults() {
        return results;
    }

}
