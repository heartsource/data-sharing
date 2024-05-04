package com.heartsrc.processor;

import com.heartsrc.examples.brandeffect.BrandEffect;
import com.heartsrc.examples.broadcast.Broadcaster;
import com.heartsrc.processor.BaseJob;

public class JobFactory {

    public static BaseJob buildJob(String type) throws Exception {
        switch (type) {
            case "brandeffect" :
                return new BrandEffect();
            case "broadcaster" :
                return new Broadcaster();
            default:
                throw new Exception("Unknown dataset type: " + type);
        }
    }
}
