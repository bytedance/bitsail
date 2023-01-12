package com.bytedance.connector.hbase.source.split;

import com.bytedance.bitsail.base.connector.reader.v1.SourceSplit;

public class HBaseSourceSplit implements SourceSplit {
    @Override
    public String uniqSplitId() {
        return null;
    }
}
