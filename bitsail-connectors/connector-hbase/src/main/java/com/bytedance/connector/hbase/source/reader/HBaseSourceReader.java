package com.bytedance.connector.hbase.source.reader;

import com.bytedance.bitsail.base.connector.reader.v1.SourceEvent;
import com.bytedance.bitsail.base.connector.reader.v1.SourcePipeline;
import com.bytedance.bitsail.base.connector.reader.v1.SourceReader;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.connector.hbase.source.split.HBaseSourceSplit;

import java.util.List;

public class HBaseSourceReader implements SourceReader<Row, HBaseSourceSplit> {
    @Override
    public void start() {

    }

    @Override
    public void pollNext(SourcePipeline<Row> pipeline) throws Exception {

    }

    @Override
    public void addSplits(List<HBaseSourceSplit> splits) {

    }

    @Override
    public boolean hasMoreElements() {
        return false;
    }

    @Override
    public void notifyNoMoreSplits() {
        SourceReader.super.notifyNoMoreSplits();
    }

    @Override
    public void handleSourceEvent(SourceEvent sourceEvent) {
        SourceReader.super.handleSourceEvent(sourceEvent);
    }

    @Override
    public List<HBaseSourceSplit> snapshotState(long checkpointId) {
        return null;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        SourceReader.super.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void close() throws Exception {

    }
}
