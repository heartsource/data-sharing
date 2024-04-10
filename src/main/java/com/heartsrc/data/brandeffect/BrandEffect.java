package com.heartsrc.data.brandeffect;

import com.heartsrc.data.BaseJob;
import com.heartsrc.data.DataAccumulator;
import com.heartsrc.data.RowResults;
import com.heartsrc.data.StringArrayKey;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.*;

public class BrandEffect extends BaseJob {
    private final static int WEIGHT_COLUMN = 5;
    private final static int AD_RESPONSE = 6;
    private final static int BRAND_RESPONSE = 7;
    private final static int MESSAGE_RESPONSE = 8;
    private final static int LIKABILITY_RESPONSE = 9;

    public static void main(String[] args) throws Exception {
        JavaSparkContext sc =  getSparkContext();
        (new BrandEffect()).runJob(sc, new String[] {"gender","broadcaster"});
        sc.close();
    }

    @Override
    protected String[] buildFactHeaders() {
        return new String[] {
                "AdRecall", "BrandRecall", "BrandLinkage",
                "MessageRecall", "MessageLinkage", "HighlyLikeable",
                "Likeable", "Unlikeable", "Neutral",
        };
    }
    @Override
    public String filePrefix() {
        return "be";
    }

    @Override
    protected RowResults computeMetrics(StringArrayKey key, DataAccumulator dataAccumulator) {
        MetricAccumulator score = (MetricAccumulator)dataAccumulator;
        BrandEffectResults res = new BrandEffectResults();
        res.grouping = key.getArray();
        res.adRecall = getPct(score.ad[0],score.ad[1]);
        res.brandRecall  = getPct(score.brand[0],score.ad[1]);
        res.brandLinkage = getPct(score.brand[0],score.brand[1]);
        res.msgRecall = getPct(score.msg[0],score.brand[1]);
        res.msgLinkage = getPct(score.msg[0],score.msg[1]);

        float wgtLike = score.like[0]+score.like[1]+score.like[2]+score.like[3]+score.like[4];
        res.highlyLikeable = getPct(score.like[4],wgtLike);
        res.likeable = getPct((score.like[4]+score.like[3]),wgtLike);
        res.unlikeable = getPct((score.like[0]+score.like[1]),wgtLike);
        res.neutral = getPct(score.like[2],wgtLike);
        return res;
    }

    @Override
    protected DataAccumulator newDataAccumulator(String[] rowData) {
        float weight = Float.parseFloat(rowData[WEIGHT_COLUMN]);
        String a = getDataString(rowData, AD_RESPONSE);
        String b = getDataString(rowData, BRAND_RESPONSE);
        String m = getDataString(rowData, MESSAGE_RESPONSE);
        String l = getDataString(rowData, LIKABILITY_RESPONSE);
        return new MetricAccumulator(weight, a, b, m, l);
    }

    @Override
    protected Map<String, Integer> getHeaders() {
        // ToDo load from the header file
        Map<String, Integer> map = new HashMap<>();
        map.put("aid", 0);
        map.put("broadcaster", 1);
        map.put("pid", 2);
        map.put("gender", 3);
        map.put("age", 4);
        map.put("weight", 5);
        map.put("ad_correct", 6);
        map.put("brand_correct", 7);
        map.put("msg_recall", 8);
        map.put("likeability", 9);
        map.put("brand", 10);
        return map;
    }


    private String getDataString(String[] data, int index) {
        return (index < (data.length)) ?  data[index]:  "";
    }
    private static class MetricAccumulator implements DataAccumulator {
        float[] ad = new float[2];
        float[] brand = new float[2];
        float[] msg = new float[2];
        float[] like = new float[5];
        MetricAccumulator(float weight, String adRecall, String brandRecall, String msgRecall, String likeabilityLevel) {
            if (!adRecall.isEmpty())
                adjustValue(adRecall, ad, weight);
            if (!brandRecall.isEmpty())
                adjustValue(brandRecall, brand, weight);
            if (!msgRecall.isEmpty())
                adjustValue(msgRecall, msg, weight);
            if (!likeabilityLevel.isEmpty()) {
                int i = Integer.parseInt(likeabilityLevel);
                adjustScaledValue(i, like, weight);
            }
        }

        @Override
        public DataAccumulator adjustValues(DataAccumulator dataAccumulator) {
            MetricAccumulator other = (MetricAccumulator)dataAccumulator;
            this.ad[0] += other.ad[0];
            this.ad[1] += other.ad[1];
            this.brand[0] += other.brand[0];
            this.brand[1] += other.brand[1];
            this.msg[0] += other.msg[0];
            this.msg[1] += other.msg[1];

            this.like[0] += other.like[0];
            this.like[1] += other.like[1];
            this.like[2] += other.like[2];
            this.like[3] += other.like[3];
            this.like[4] += other.like[4];
            return this;
        }

        private void adjustScaledValue(int scaleLevel, float[] ary, float weight) {
            ary[scaleLevel-1] += weight;
        }

        private void adjustValue(String response, float[] ary, float value) {
            ary[1] += value;
            if (response.equals("Y")) ary[0] += value;
        }

    }



}

