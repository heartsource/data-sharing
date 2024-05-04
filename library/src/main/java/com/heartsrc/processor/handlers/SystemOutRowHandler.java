package com.heartsrc.processor.handlers;

import org.apache.commons.lang3.StringUtils;

public class SystemOutRowHandler implements RowHandlerI {
    @Override
    public void handle(Object[] row) {
        System.out.println(StringUtils.join(row, ","));
    }

}
