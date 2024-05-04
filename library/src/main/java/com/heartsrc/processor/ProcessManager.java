package com.heartsrc.processor;

import com.heartsrc.dto.DatasetDefinition;
import com.heartsrc.processor.handlers.CollectionResultsHandler;
import com.heartsrc.processor.handlers.ResultsHandlerI;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

public class ProcessManager {
    private static JavaSparkContext sc = BaseJob.getSparkContext();

    public void run(DatasetDefinition def) throws Exception {
        CollectionResultsHandler collector = new CollectionResultsHandler();
        run(def, collector);
        List<Object[]> res = collector.getResults();
        for (Object[] objects: res){
            for (Object obj: objects) {
                System.out.print(obj+",");
            }
            System.out.println();
        }
    }
    public void run(DatasetDefinition def, ResultsHandlerI collector) throws Exception {
        validateRequest(def);
        BaseJob job = JobFactory.buildJob(def.getType());
        /*
        TODO:
        job.setFilters();
        job.setAdjusters();
        job.setRowHandler();
         */
        job.setResultsHandler(collector);
        job.runJob(sc, def.getGroupByFields());
    }

    private void validateRequest(DatasetDefinition def)  throws Exception {
        if (def.getGroupByFields() == null) {
            throw new Exception("No grouped columns provided");
        }

    }


}
