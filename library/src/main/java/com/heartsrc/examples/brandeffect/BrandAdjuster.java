package com.heartsrc.examples.brandeffect;

import com.heartsrc.utils.ValueAdjust;

import java.util.Map;

public class BrandAdjuster implements ValueAdjust {

    public BrandAdjuster(String brandName, boolean notEval, String adjusted) {
        this.brandName = brandName;
        this.notEval = notEval;
        this.adjusted = adjusted;
    }
    String adjusted = null;
    String brandName = null;
    boolean notEval = false;
    @Override
    public String[] adjustRowValues(String[] inRow, Map<String, Integer> headers) {
        Integer index = headers.get("brand");
        if (index != null) {
            if (inRow[index] != null) {
                if (notEval) {
                    inRow[index] = inRow[index].equals(brandName) ? inRow[index] : adjusted;
                } else {
                    inRow[index] = inRow[index].equals(brandName) ? adjusted : inRow[index];
                }
            }
        }
        return inRow;
    }
}
