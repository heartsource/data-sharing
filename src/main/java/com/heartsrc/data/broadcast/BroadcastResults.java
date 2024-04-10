package com.heartsrc.data.broadcast;

import com.heartsrc.data.RowResults;

public class BroadcastResults extends RowResults {
    public float news_score = 0;
    public float comedy_score = 0;
    public float documentary_score = 0;

    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("[{").append("NewsScore=").append(news_score).append("};");
        sb.append("{").append("ComedyScore=").append(comedy_score).append("};");
        sb.append("{").append("DocumentaryScore=").append(documentary_score).append("}]");
        return  super.toString() + sb.toString();
    }

    @Override
    public Object[] getRowData() {
        Object[] data = new Object[grouping.length + 3];
        int i =0;
        for (String s : grouping) {
            data[i++] = s;
        }
        data[i++] = news_score;
        data[i++] = comedy_score;
        data[i++] = documentary_score;
        return data;
    }

}
