package com.heartsrc.loaders;

import org.apache.spark.api.java.JavaSparkContext;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Loads data from file to build the DataDefinition that contains with the metadata
 * about the dataset and the
 */
public class CommaSeparatedDataLoader extends DataLoader {
    private String filename;
    private String fileExtension;

    public CommaSeparatedDataLoader(String filename, String fileExtension) {
        this.filename = filename;
        this.fileExtension = fileExtension;
    }
    private String dataFileName() {
        return filename +"." + fileExtension;
    }

    @Override
    public DataDefinition buildDataDefinition(JavaSparkContext sc) {
        DataDefinition dataDefinition = new DataDefinition();
        dataDefinition.setColumnHeaders(getHeaders());
        dataDefinition.setInitialRDD(sc.textFile(dataFileName()).map(val -> val.split(",")));
        return dataDefinition;
    }

    private Map<String, Integer> getHeaders() {
        BufferedReader reader;
        Map<String, Integer> retMap = new HashMap<>();
        try {
            reader = new BufferedReader(new FileReader(filename +"-header." + fileExtension));
            String line = reader.readLine();
            String[] split = line.split(",");
            for (int i=0; i< split.length; i++) {
                retMap.put(split[i],i);
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return retMap;
    }
}
