package com.heartsrc.processor.handlers;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;

import java.io.FileWriter;
import java.util.List;

public class FileResultsHandler implements ResultsHandlerI {
    private String outputFilename = "";

    public FileResultsHandler(String outputFilename) {
        this.outputFilename = outputFilename;
    }
    @Override
    public void handle(JavaRDD<Object[]> resultRDD, String[] resultHeaders) throws Exception{
        List<Object[]> rowList = resultRDD.collect();
        FileWriter fw = new FileWriter(outputFilename+".out");
        fw.write(StringUtils.join(resultHeaders, ",")+"\n");
        for (Object[] row: rowList) {
            fw.write(StringUtils.join(row, ",")+"\n");
        }
        fw.flush();
        fw.close();
    }

}
