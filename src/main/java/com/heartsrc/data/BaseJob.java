package com.heartsrc.data;

import com.heartsrc.utils.SystemOutProcessor;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.FileWriter;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class BaseJob implements Serializable {

    private Map<String, Set<String>> filters = null;

    private List<ValueAdjust> adjusters = null;
    protected abstract Map<String, Integer> getHeaders();

    public abstract String filePrefix();

    protected String dataFileName() {
        return "src/main/resources/" + filePrefix() + ".txt";
    }

    protected abstract String[] buildFactHeaders();

    public void runJob(JavaSparkContext sc, String[] keyColumns) throws Exception {
        RowResultProcessor rowProcessor = rowProcessor();
        Map<String, Integer> headers = getHeaders();
        JavaRDD<String> initRdd = sc.textFile(dataFileName());
        JavaRDD<String[]> rawData = initRdd.map(val -> val.split(","));
        if (filters != null) {
            rawData = rawData.filter(row -> useRow(row, headers));
        }
        if ((adjusters != null) && (!adjusters.isEmpty())) {
            rawData = rawData.map(row -> adjustRow(row, headers));
        }
        JavaRDD<Object[]> result = computeRows(keyColumns, rawData, headers);
        String[] resultHeaders = appendArray(keyColumns, buildFactHeaders());
        System.out.println(StringUtils.join(resultHeaders, ","));
        if (rowProcessor != null)
            result.foreach(rowProcessor::process);
        complete(result, resultHeaders);

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


    protected void complete(JavaRDD<Object[]> resultRDD,  String[] resultHeaders) throws Exception{
        toFile(resultRDD,  resultHeaders);
    }
    private void toFile(JavaRDD<Object[]> resultRDD,  String[] resultHeaders) throws Exception{
        List<Object[]> rowList = resultRDD.collect();
        FileWriter fw = new FileWriter(filePrefix()+".out");
        fw.write(StringUtils.join(resultHeaders, ",")+"\n");
        for (Object[] row: rowList) {
            fw.write(StringUtils.join(row, ",")+"\n");
        }
        fw.flush();
        fw.close();
    }

    protected RowResultProcessor rowProcessor() {
        return new SystemOutProcessor();
    }

    protected JavaRDD<Object[]> computeRows(String[] keyColumns, JavaRDD<String[]> rawData, Map<String, Integer> headers) {
        return rawData
                .mapToPair(row -> buildMapPair(row, headers, keyColumns))
                .reduceByKey(DataAccumulator::adjustValues)
                .map(keyValue -> computeMetrics(keyValue._1, keyValue._2).getRowData());
    }

    protected Tuple2<StringArrayKey, DataAccumulator> buildMapPair(String[] data, Map<String, Integer> headers, String[] keyColumns) {
        return new Tuple2<StringArrayKey, DataAccumulator>(
                new StringArrayKey(populateDataValues(data, headers, keyColumns)),
                newDataAccumulator(data));
    }

    protected abstract RowResults computeMetrics(StringArrayKey key, DataAccumulator dataAccumulator);

    protected abstract  DataAccumulator newDataAccumulator(String[] rowData);

    protected String[] appendArray(String[] a, String[] b) {
        int aLen = a.length;
        int bLen = b.length;
        String[] c = new String[aLen + bLen];
        System.arraycopy(a, 0, c, 0, aLen);
        System.arraycopy(b, 0, c, aLen, bLen);
        return c;
    }
    protected float getPct(float numerator, float denominator) {
        return getPct(numerator, denominator, 0);
    }
    protected float getPct(float numerator, float denominator, float divByZero) {
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

    protected static SparkSession getSparkSession() {
        System.setProperty("hadoop.home.dir", "C:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
                .config("spark.sql.warehouse.dir","file:///c:/tmp/")
                .getOrCreate();
        return spark;
    }

    public static JavaSparkContext getSparkContext() {
        System.setProperty("hadoop.home.dir", "c:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        return  new JavaSparkContext(conf);
    }

}
