package com.heartsrc.data.brandeffect;

import com.heartsrc.data.RowResults;

public class BrandEffectResults extends RowResults {
    public float adRecall = 0;
    public float brandRecall = 0;
    public float brandLinkage = 0;
    public float msgRecall = 0;
    public float msgLinkage = 0;

    public float highlyLikeable = 0;
    public float likeable = 0;
    public float unlikeable = 0;
    public float neutral = 0;

    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("[{").append("AdRecall=").append(adRecall*100).append("};");
        sb.append("{").append("BrandRecall=").append(brandRecall*100).append("};");
        sb.append("{").append("BrandLinkage=").append(brandLinkage*100).append("};");
        sb.append("{").append("MessageRecall=").append(msgRecall*100).append("};");
        sb.append("{").append("MessageLinkage=").append(msgLinkage*100).append("};");
        sb.append("{").append("HighlyLikeable=").append(highlyLikeable*100).append("};");
        sb.append("{").append("Likeable=").append(likeable*100).append("};");
        sb.append("{").append("Unlikeable=").append(unlikeable*100).append("};");
        sb.append("{").append("Neutral=").append(neutral*100).append("}]");
        return  super.toString() + sb.toString();
    }
    @Override
    public Object[] getRowData() {
        Object[] data = new Object[grouping.length + 9];
        int i =0;
        for (String s : grouping) {
            data[i++] = s;
        }
        data[i++] = adRecall;
        data[i++] = brandRecall;
        data[i++] = brandLinkage;
        data[i++] = msgRecall;
        data[i++] = msgLinkage;
        data[i++] = highlyLikeable;
        data[i++] = likeable;
        data[i++] = unlikeable;
        data[i++] = neutral;
        return data;
    }
}
