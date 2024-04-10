package com.heartsrc.data;

import com.heartsrc.data.brandeffect.BrandAdjuster;
import com.heartsrc.data.brandeffect.BrandEffect;
import com.heartsrc.data.broadcast.Network;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.*;

public class DataCombiner  {
    public static void main(String[] args) throws Exception {
        DataCombiner dc = new DataCombiner();
        dc.runJobs();
    }
    public void runJobs() throws Exception {
        testRun();
    }
    public void testRun() throws Exception {
        JavaSparkContext sc = BaseJob.getSparkContext();
        BrandEffect be = new BrandEffect();
        be.setFilters(maleFilter());
        be.setAdjusters(notFord());
        (be).runJob(sc, new String[] {"gender","broadcaster","brand","aid"});
        Network net = new Network();
//        net.setFilters(rightLeaning());
        (net).runJob(sc, new String[] {"broadcaster","politics"});
        sc.close();
        SparkSession session = BaseJob.getSparkSession();

        Dataset<Row> beDataFrame = session.read().option("header", true).csv("be.out");
        beDataFrame.createOrReplaceTempView("brandeffect_table");

        Dataset<Row> broadcasterDataFrame = session.read().option("header", true).csv("network-attribs.out");
        broadcasterDataFrame.createOrReplaceTempView("broadcaster_table");
        Dataset<Row> resultSet = session.sql(
                "select * " +
                        "from brandeffect_table be, broadcaster_table bc " +
                        "where be.broadcaster = bc.broadcaster");
        resultSet.show();

    }

    private List<ValueAdjust> notFord() {
        List<ValueAdjust> list = new ArrayList<>();
        list.add(new BrandAdjuster("Ford", true, "Competitor"));
        return list;
    }

    private Map<String, Set<String>> maleFilter() {
        Set<String> vals = new HashSet<>();
        vals.add("M");
        Map<String, Set<String>> ret = new HashMap<>();
        ret.put("gender", vals);
        return ret;
    }

    private Map<String, Set<String>> rightLeaning() {
        Set<String> vals = new HashSet<>();
        vals.add("right");
        vals.add("right_leaning");
        Map<String, Set<String>> ret = new HashMap<>();
        ret.put("politics", vals);
        return ret;
    }
}
