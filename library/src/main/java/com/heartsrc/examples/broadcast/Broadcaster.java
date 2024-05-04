package com.heartsrc.examples.broadcast;

import com.heartsrc.loaders.CommaSeparatedDataLoader;
import com.heartsrc.loaders.DataLoader;
import com.heartsrc.processor.BaseJob;
import com.heartsrc.processor.RowResults;
import com.heartsrc.processor.StatsAccumulator;
import com.heartsrc.utils.StringArrayKey;
import org.apache.spark.api.java.JavaSparkContext;

public class Broadcaster extends BaseJob {
    private final static int NEWS = 3;
    private final static int COMEDY = 4;
    private final static int DOCUMENTARY = 5;

    public static void main(String[] args) throws Exception {
        JavaSparkContext sc =  getSparkContext();
        (new Broadcaster()).runJob(sc, new String[] {"network","politics"});
        sc.close();
    }

    @Override
    protected String[] buildFactHeaders() {
        return new String[] {"NewsScore", "ComedyScore", "DocumentaryScore"};
    }

    @Override
    protected DataLoader getDataLoader() {
        return new CommaSeparatedDataLoader( "src/main/resources/network-attribs","txt");
    }


    @Override
    protected RowResults computeMetrics(StringArrayKey key, StatsAccumulator dataAccumulator) {
        MetricAccumulator score = (MetricAccumulator)dataAccumulator;
        BroadcastResults res = new BroadcastResults();
        res.grouping = key.getArray();
        score.processResult(res);
        return res;
    }

    @Override
    protected StatsAccumulator newDataAccumulator(String[] rowData) {
        float news = Float.parseFloat(rowData[NEWS]);
        float comedy = Float.parseFloat(rowData[COMEDY]);
        float documentary = Float.parseFloat(rowData[DOCUMENTARY]);
        return new MetricAccumulator(news, comedy, documentary);
    }

    private static class MetricAccumulator implements StatsAccumulator {
        float news_scr = 0;
        float comedy_scr = 0;
        float documentary_scr = 0;
        MetricAccumulator(float news_scr, float comedy_scr, float documentary_scr) {
            this.news_scr = news_scr;
            this.comedy_scr = comedy_scr;
            this.documentary_scr = documentary_scr;
        }

        @Override
        public StatsAccumulator adjustValues(StatsAccumulator dataAccumulator) {
            MetricAccumulator other = (MetricAccumulator)dataAccumulator;
            this.news_scr += other.news_scr;
            this.comedy_scr += other.comedy_scr;
            this.documentary_scr += other.documentary_scr;
            return this;
        }

        @Override
        public void processResult(RowResults rowResults) {
            BroadcastResults res = (BroadcastResults)rowResults;
            res.news_score = news_scr;
            res.comedy_score  = comedy_scr;
            res.documentary_score  = documentary_scr;
        }
    }
}

