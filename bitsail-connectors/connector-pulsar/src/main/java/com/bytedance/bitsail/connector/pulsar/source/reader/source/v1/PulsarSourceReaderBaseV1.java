/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bytedance.bitsail.connector.pulsar.source.reader.source.v1;

import com.bytedance.bitsail.base.connector.reader.v1.SourcePipeline;
import com.bytedance.bitsail.base.connector.reader.v1.SourceReader;
import com.bytedance.bitsail.base.format.DeserializationSchema;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.connector.pulsar.source.config.SourceConfiguration;
import com.bytedance.bitsail.connector.pulsar.source.enumerator.topic.TopicPartition;
import com.bytedance.bitsail.connector.pulsar.source.reader.deserializer.v1.PulsarDeserializationSchema;
import com.bytedance.bitsail.connector.pulsar.source.reader.emitter.PulsarRecordEmitter;
import com.bytedance.bitsail.connector.pulsar.source.reader.fetcher.PulsarFetcherManagerBase;
import com.bytedance.bitsail.connector.pulsar.source.reader.message.PulsarMessage;
import com.bytedance.bitsail.connector.pulsar.source.reader.split.v1.PulsarPartitionSplitReader;
import com.bytedance.bitsail.connector.pulsar.source.split.v1.PulsarPartitionSplit;
import com.bytedance.bitsail.connector.pulsar.source.split.v1.PulsarPartitionSplitState;

import com.google.common.collect.Sets;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SourceReaderBase;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.InputStatus;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkState;

abstract class PulsarSourceReaderBaseV1 implements SourceReader<Row, PulsarPartitionSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(SourceReaderBase.class);

    protected final SourceConfiguration sourceConfiguration;
    protected final PulsarClient pulsarClient;
    protected final PulsarAdmin pulsarAdmin;
    private final FutureCompletingBlockingQueue<RecordsWithSplitIds<PulsarMessage<byte[]>>> elementsQueue;
    private final PulsarFetcherManagerBase<Row> splitFetcherManager = null;
    private final PulsarRecordEmitter<Object> recordEmitter;
    private final HashMap<String, SplitContext<PulsarPartitionSplit, PulsarPartitionSplitState>> splitStates;
    private final BitSailConfiguration readerConfiguration;
    private final transient DeserializationSchema<byte[], Row> deserializationSchema;
    private final Context context;
    private final Supplier<? extends PulsarPartitionSplitReader> splitReaderSupplier;

    /** The latest fetched batch of records-by-split from the split reader. */
    @Nullable private RecordsWithSplitIds<PulsarMessage<byte[]>> currentFetch;

    @Override
    public List<PulsarPartitionSplit> snapshotState(long checkpointId) {
        List<PulsarPartitionSplit> splits = new ArrayList<>();
        splitStates.forEach((id, context) -> splits.add(toSplitType(id, context.state)));
        return splits;
    }

    @Nullable private SplitContext<PulsarPartitionSplit, PulsarPartitionSplitState> currentSplitContext;
    @Nullable private SourceOutput<PulsarPartitionSplit> currentSplitOutput;
    private boolean noMoreSplitsAssignment;
    private final transient Set<PulsarPartitionSplit> assignedPulsarSplits;
    private final Map<String, PulsarPartitionSplitReader> splitReaderMapping = new HashMap<>();
    private final Map<Integer, Boolean> fetcherStatus = new HashMap<>();

    protected PulsarSourceReaderBaseV1(
        FutureCompletingBlockingQueue<RecordsWithSplitIds<PulsarMessage<byte[]>>> elementsQueue,
        Supplier<? extends PulsarPartitionSplitReader> splitReaderSupplier,
        BitSailConfiguration readerConfiguration,
        Context context,
        SourceConfiguration sourceConfiguration,
        PulsarClient pulsarClient,
        PulsarAdmin pulsarAdmin) {

        this.elementsQueue = elementsQueue;
        this.splitReaderSupplier = splitReaderSupplier;
        this.recordEmitter = new PulsarRecordEmitter<>();
        this.splitStates = new HashMap<>();
        this.readerConfiguration = readerConfiguration;
        this.context = context;
        this.noMoreSplitsAssignment = false;
        this.deserializationSchema = new PulsarDeserializationSchema(
            readerConfiguration,
            context.getTypeInfos(),
            context.getFieldNames());
        this.sourceConfiguration = sourceConfiguration;
        this.pulsarClient = pulsarClient;
        this.pulsarAdmin = pulsarAdmin;
        this.assignedPulsarSplits = Sets.newHashSet();
    }

    protected PulsarPartitionSplitState initializedState(PulsarPartitionSplit split) {
        return new PulsarPartitionSplitState(split);
    }

    protected PulsarPartitionSplit toSplitType(
            String splitId, PulsarPartitionSplitState splitState) {
        return splitState.toPulsarPartitionSplit();
    }

    @Override
    public void addSplits(List<PulsarPartitionSplit> splits) {
        LOG.info("Adding split(s) to reader: {}", splits);
        // Initialize the state for each split.
        splits.forEach(
            s -> {
                splitStates.put(s.getPartition().toString(), new SplitContext<>(s.splitId(), initializedState(s)));
                PulsarPartitionSplitReader splitReader = getOrCreateSplitReader(s.splitId());
                splitReader.handleSplitsChanges(new SplitsAddition<>(splits));
            });
        // Hand over the splits to the split fetcher to start fetch.
        assignedPulsarSplits.addAll(splits);
    }

    protected PulsarPartitionSplitReader getOrCreateSplitReader(String splitId) {
        PulsarPartitionSplitReader splitReader = splitReaderMapping.get(splitId);

        if (splitReader == null) {
            splitReader = splitReaderSupplier.get();
            splitReaderMapping.put(splitId, splitReader);
        }

        return splitReader;
    }

    @Override
    public boolean hasMoreElements() {
        return true;
    }

    @Override
    public void close() throws Exception {
        // Close the all the consumers first.
        for (PulsarPartitionSplitReader splitReader : splitReaderMapping.values()) {
            splitReader.close();
        }

        // Close shared pulsar resources.
        pulsarClient.shutdown();
        pulsarAdmin.close();
    }

    @Override
    public void pollNext(SourcePipeline<Row> output) throws Exception {

        for (PulsarPartitionSplitReader splitReader : splitReaderMapping.values()) {
            RecordsWithSplitIds<PulsarMessage<byte[]>> recordsWithSplitId = splitReader.fetch();

            if (recordsWithSplitId == null) {
                continue;
            }

            while (recordsWithSplitId.nextSplit() != null) {
                PulsarMessage<byte[]> record = recordsWithSplitId.nextRecordFromSplit();

                while (record != null) {
                    Row deserialize = deserializationSchema.deserialize(record.getValue());
                    output.output(deserialize);
                    LOG.trace("Emitted record: {}", record);
                    record = recordsWithSplitId.nextRecordFromSplit();
                }
            }
        }
    }

    private InputStatus trace(InputStatus status) {
        LOG.trace("Source reader status: {}", status);
        return status;
    }

    @Nullable
    private RecordsWithSplitIds<PulsarMessage<byte[]>> getNextFetch(final SourcePipeline<Row> output) {
        splitFetcherManager.checkErrors();

        LOG.trace("Getting next source data batch from queue");
        final RecordsWithSplitIds<PulsarMessage<byte[]>> recordsWithSplitId = elementsQueue.poll();
        if (recordsWithSplitId == null || !moveToNextSplit(recordsWithSplitId, output)) {
            // No element available, set to available later if needed.
            return null;
        }

        currentFetch = recordsWithSplitId;
        return recordsWithSplitId;
    }

    private void finishCurrentFetch(
        final RecordsWithSplitIds<PulsarMessage<byte[]>> fetch, final SourcePipeline<Row> output) {
        currentFetch = null;
        currentSplitContext = null;
        currentSplitOutput = null;

        final Set<String> finishedSplits = fetch.finishedSplits();
        if (!finishedSplits.isEmpty()) {
            LOG.info("Finished reading split(s) {}", finishedSplits);
            Map<String, PulsarPartitionSplitState> stateOfFinishedSplits = new HashMap<>();
            for (String finishedSplitId : finishedSplits) {
                stateOfFinishedSplits.put(
                    finishedSplitId, splitStates.remove(finishedSplitId).state);
            }
            onSplitFinished(stateOfFinishedSplits);
        }

        fetch.recycle();
    }

    protected void onSplitFinished(Map<String, PulsarPartitionSplitState> stateOfFinishedSplits) {};

    private boolean moveToNextSplit(
        RecordsWithSplitIds<PulsarMessage<byte[]>> recordsWithSplitIds, SourcePipeline<Row> output) {
        final String nextSplitId = recordsWithSplitIds.nextSplit();
        if (nextSplitId == null) {
            LOG.trace("Current fetch is finished.");
            finishCurrentFetch(recordsWithSplitIds, output);
            return false;
        }

        currentSplitContext = splitStates.get(nextSplitId);
        checkState(currentSplitContext != null, "Have records for a split that was not registered");
        LOG.trace("Emitting records from fetch for split {}", nextSplitId);
        return true;
    }

    public CompletableFuture<Void> isAvailable() {
        return currentFetch != null
            ? FutureCompletingBlockingQueue.AVAILABLE
            : elementsQueue.getAvailabilityFuture();
    }


    private InputStatus finishedOrAvailableLater() {
        final boolean allFetchersHaveShutdown = splitFetcherManager.maybeShutdownFinishedFetchers();
        if (!(noMoreSplitsAssignment && allFetchersHaveShutdown)) {
            return InputStatus.NOTHING_AVAILABLE;
        }
        if (elementsQueue.isEmpty()) {
            // We may reach here because of exceptional split fetcher, check it.
            splitFetcherManager.checkErrors();
            return InputStatus.END_OF_INPUT;
        } else {
            // We can reach this case if we just processed all data from the queue and finished a
            // split,
            // and concurrently the fetcher finished another split, whose data is then in the queue.
            return InputStatus.MORE_AVAILABLE;
        }
    }

    public void commitCursors(Map<TopicPartition, MessageId> cursors) {
        cursors.forEach((partition, messageId) -> {

            PulsarPartitionSplitReader splitReader = getOrCreateSplitReader(partition.toString());
            splitReader.commit(partition, messageId);
        });
    }

    private static final class SplitContext<T, PulsarPartitionSplitState> {

        final String splitId;
        final PulsarPartitionSplitState state;
        SourceOutput<T> sourceOutput;

        private SplitContext(String splitId, PulsarPartitionSplitState state) {
            this.state = state;
            this.splitId = splitId;
        }

        SourceOutput<T> getOrCreateSplitOutput(ReaderOutput<T> mainOutput) {
            if (sourceOutput == null) {
                sourceOutput = mainOutput.createOutputForSplit(splitId);
            }
            return sourceOutput;
        }
    }
}
