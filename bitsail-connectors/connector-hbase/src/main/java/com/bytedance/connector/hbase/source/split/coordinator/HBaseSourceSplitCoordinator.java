package com.bytedance.connector.hbase.source.split.coordinator;

import com.bytedance.bitsail.base.connector.reader.v1.SourceEvent;
import com.bytedance.bitsail.base.connector.reader.v1.SourceSplitCoordinator;

import javax.annotation.Nullable;
import java.util.List;

public class HBaseSourceSplitCoordinator implements SourceSplitCoordinator {
    @Override
    public void start() {

    }

    @Override
    public void addReader(int subtaskId) {

    }

    @Override
    public void addSplitsBack(List splits, int subtaskId) {

    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {

    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        SourceSplitCoordinator.super.handleSourceEvent(subtaskId, sourceEvent);
    }

    @Override
    public Object snapshotState() throws Exception {
        return null;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        SourceSplitCoordinator.super.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void close() {

    }
}
