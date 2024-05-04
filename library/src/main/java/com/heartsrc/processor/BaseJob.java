package com.heartsrc.processor;

import com.heartsrc.loaders.DataDefinition;
import com.heartsrc.loaders.DataLoader;
import com.heartsrc.processor.handlers.ResultsHandlerI;
import com.heartsrc.processor.handlers.RowHandlerI;
import com.heartsrc.processor.handlers.SystemOutResultsHandler;
import com.heartsrc.utils.StringArrayKey;
import com.heartsrc.utils.ValueAdjust;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This abstract class is to be extended Implementation for specific data inputs.
 */
public abstract class BaseJob implements Serializable {
    private ResultsHandlerI resultsHandler = new SystemOutResultsHandler();

    private Map<String, Set<String>> filters = null;
    private List<ValueAdjust> adjusters = null;

//    protected abstract  String outputFilename();

    /**
     * builds the Object to assist with holding the running totals for the fact data.
     * Can be used to convert the rows column to the needed data type.  These
     * objects with be created in the map to key pair as the value of the key which will
     * be the group columns -- see computeMetrics
     * @param rowData row of data
     * @return object for the converted row data that know how reduce the values
     * in the reduce by key process
     */
    protected abstract StatsAccumulator newDataAccumulator(String[] rowData);

    /**
     * The StatsAccumulator is object created by the newDataAccumulator() method during the mapToPair
     * to build an object used in the value of the mapped pair.  This object implementation has an
     * adjustValues() method that is performed on the reduceByKey.
     */
    protected abstract RowResults computeMetrics(StringArrayKey key, StatsAccumulator dataAccumulator);


    /**
     * Provides the String[] of the column headers the resulting fact columns (Stats/Metrics/etc...) that
     * is to be injected into the final results RDD that is included by the StatsAccumulator of the implementation
     * @return String[] of new column headers (produced as facts)
     */
    protected abstract String[] buildFactHeaders();
    protected abstract DataLoader getDataLoader();

    public void setResultsHandler(ResultsHandlerI resultsHandler) {
        this.resultsHandler = resultsHandler;
    }

    public void runJob(JavaSparkContext sc, String[] keyColumns) throws Exception {
        DataLoader ldr = getDataLoader();
        DataDefinition def = ldr.buildDataDefinition(sc);
        JavaRDD<String[]> rawData = def.getInitialRDD();
        Map<String, Integer> headers = def.getColumnHeaders();

        if (filters != null) {
            rawData = rawData.filter(row -> useRow(row, headers));
        }
        if ((adjusters != null) && (!adjusters.isEmpty())) {
            rawData = rawData.map(row -> adjustRow(row, headers));
        }
        JavaRDD<Object[]> result = computeRows(keyColumns, rawData, headers);
        String[] resultHeaders = appendArray(keyColumns, buildFactHeaders());
        RowHandlerI handler = getRowHandler();
        if (handler != null)
            result.foreach(handler::handle);

        resultsHandler.handle(result, resultHeaders);

    }

    private String[] adjustRow(String[] row, Map<String, Integer> headers) {
        for (ValueAdjust adj : adjusters)
            row = adj.adjustRowValues(row, headers);
        return row;
    }

    private boolean useRow(String[] row, Map<String, Integer> headers) {
        for (Map.Entry<String, Set<String>> fltrEntry : filters.entrySet()) {
            String key = fltrEntry.getKey();
            Integer dataIndex = headers.get(key);
            if (dataIndex == null)
                return false;
            String dataValue = row[dataIndex];
            Set<String> acceptableValues = fltrEntry.getValue();
            if (!acceptableValues.contains(dataValue))
                return false;
        }
        return true;
    }

    public void setFilters(Map<String, Set<String>> filters) {
        this.filters = filters;
    }
    public void setAdjusters(List<ValueAdjust> adjusters) {
        this.adjusters = adjusters;
    }

    public RowHandlerI getRowHandler() {
        return rowHandler;
    }

    public void setRowHandler(RowHandlerI rowHandler) {
        this.rowHandler = rowHandler;
    }

    private RowHandlerI rowHandler = null;


    protected JavaRDD<Object[]> computeRows(String[] keyColumns, JavaRDD<String[]> rawData, Map<String, Integer> headers) {
        return rawData
                .mapToPair(row -> buildMapPair(row, headers, keyColumns))
                .reduceByKey(StatsAccumulator::adjustValues)
                .map(keyValue -> computeMetrics(keyValue._1, keyValue._2).getRowData());
    }

    protected Tuple2<StringArrayKey, StatsAccumulator> buildMapPair(String[] data, Map<String, Integer> headers, String[] keyColumns) {
        return new Tuple2<>(
                new StringArrayKey(populateDataValues(data, headers, keyColumns)),
                newDataAccumulator(data));
    }


    protected String[] appendArray(String[] a, String[] b) {
        int aLen = a.length;
        int bLen = b.length;
        String[] c = new String[aLen + bLen];
        System.arraycopy(a, 0, c, 0, aLen);
        System.arraycopy(b, 0, c, aLen, bLen);
        return c;
    }
    protected static float getPct(float numerator, float denominator) {
        return getPct(numerator, denominator, 0);
    }
    protected static float getPct(float numerator, float denominator, float divByZero) {
        return (denominator!=0) ? numerator/denominator : divByZero;
    }

    protected static String[] populateDataValues(String[] data, Map<String, Integer> headers, String[] keyColumns) {
        String[] keyArray = new String[0];
        if ((keyColumns != null) && (keyColumns.length > 0)) {
            keyArray = new String[keyColumns.length];
            for (int i = 0; i< keyColumns.length; i++) {
                Integer index = headers.get(keyColumns[i]);
                if (index != null) {
                    keyArray[i] = data[index];
                } else {
                    keyArray[i] = keyColumns[i] + " Not Found";
                }
            }
        }
        return keyArray;
    }


    public static JavaSparkContext getSparkContext() {
        System.setProperty("hadoop.home.dir", "c:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        return  new JavaSparkContext(conf);
    }
}
