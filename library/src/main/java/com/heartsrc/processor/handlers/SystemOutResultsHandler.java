package com.heartsrc.processor.handlers;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;

import java.io.FileWriter;
import java.util.List;

public class SystemOutResultsHandler implements ResultsHandlerI {
    @Override
    public void handle(JavaRDD<Object[]> resultRDD, String[] resultHeaders) throws Exception{
        List<Object[]> rowList = resultRDD.collect();
        System.out.println(StringUtils.join(resultHeaders, ","));
        for (Object[] row: rowList) {
            System.out.println(StringUtils.join(row, ","));
        }
    }

}
