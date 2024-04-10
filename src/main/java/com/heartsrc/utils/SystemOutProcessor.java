package com.heartsrc.utils;

import com.heartsrc.data.RowResultProcessor;
import org.apache.commons.lang.StringUtils;

public class SystemOutProcessor implements RowResultProcessor {
    @Override
    public void process(Object[] row) {
        System.out.println(StringUtils.join(row, ","));
    }

}
