package com.heartsrc.data.broadcast;

import com.heartsrc.data.BaseJob;
import com.heartsrc.data.DataAccumulator;
import com.heartsrc.data.RowResults;
import com.heartsrc.data.StringArrayKey;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.HashMap;
import java.util.Map;

public class Network extends BaseJob {
    private final static int NEWS = 3;
    private final static int COMEDY = 4;
    private final static int DOCUMENTARY = 5;

    public static void main(String[] args) throws Exception {
        JavaSparkContext sc =  getSparkContext();
        (new Network()).runJob(sc, new String[] {"network","politics"});
        sc.close();
    }

    @Override
    protected String[] buildFactHeaders() {
        return new String[] {"NewsScore", "ComedyScore", "DocumentaryScore"};
    }

    @Override
    public String filePrefix() {
        return "network-attribs";
    }


    @Override
    protected RowResults computeMetrics(StringArrayKey key, DataAccumulator dataAccumulator) {
        MetricAccumulator score = (MetricAccumulator)dataAccumulator;
        BroadcastResults res = new BroadcastResults();
        res.grouping = key.getArray();
        res.news_score = score.news_scr;
        res.comedy_score  = score.comedy_scr;
        res.documentary_score  = score.documentary_scr;
        return res;
    }

    @Override
    protected DataAccumulator newDataAccumulator(String[] rowData) {
        float news = Float.parseFloat(rowData[NEWS]);
        float comedy = Float.parseFloat(rowData[COMEDY]);
        float documentary = Float.parseFloat(rowData[DOCUMENTARY]);
        return new MetricAccumulator(news, comedy, documentary);
    }

    @Override
    protected Map<String, Integer> getHeaders() {
        // ToDo load from the header file
        Map<String, Integer> map = new HashMap<>();
        map.put("broadcaster", 0);
        map.put("network", 1);
        map.put("politics", 2);
        map.put("news_scr", 3);
        map.put("comedy_scr", 4);
        map.put("documentary_scr", 5);
        return map;
    }

    private static class MetricAccumulator implements DataAccumulator {
        float news_scr = 0;
        float comedy_scr = 0;
        float documentary_scr = 0;
        MetricAccumulator(float news_scr, float comedy_scr, float documentary_scr) {
            this.news_scr = news_scr;
            this.comedy_scr = comedy_scr;
            this.documentary_scr = documentary_scr;
        }

        @Override
        public DataAccumulator adjustValues(DataAccumulator dataAccumulator) {
            MetricAccumulator other = (MetricAccumulator)dataAccumulator;
            this.news_scr += other.news_scr;
            this.comedy_scr += other.comedy_scr;
            this.documentary_scr += other.documentary_scr;
            return this;
        }
    }
}

