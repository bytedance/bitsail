package com.bytedance.bitsail.connector.hbase.source.reader;

import com.bytedance.bitsail.base.connector.reader.v1.SourceEvent;
import com.bytedance.bitsail.base.connector.reader.v1.SourcePipeline;
import com.bytedance.bitsail.base.connector.reader.v1.SourceReader;
import com.bytedance.bitsail.base.format.DeserializationFormat;
import com.bytedance.bitsail.base.format.DeserializationSchema;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.constants.Constants;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.common.util.Preconditions;
import com.bytedance.bitsail.connector.hbase.HBaseHelper;
import com.bytedance.bitsail.connector.hbase.format.HBaseDeserializationFormat;
import com.bytedance.bitsail.connector.hbase.error.HBasePluginErrorCode;
import com.bytedance.bitsail.connector.hbase.option.HBaseReaderOptions;
import com.bytedance.bitsail.connector.hbase.source.split.HBaseSourceSplit;

import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.collect.Maps;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class HBaseSourceReader implements SourceReader<Row, HBaseSourceSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseSourceReader.class);
    private static final String ROW_KEY = "rowkey";
    private final int subTaskId;

    /**
     * InputFormat natively supported by HBase.
     */
    private transient TableInputFormat tableInputFormat;

    /**
     * Fake JobContext for constructing {@link TableInputFormat}.
     */
    private transient JobContext jobContext;

    /**
     * Used to de duplicate user-defined fields with the same name.
     */
    private transient Map<String, byte[][]> namesMap;

    /**
     * Schema Settings.
     */
    private String tableName;

    private transient Result value;

    private TypeInfo<?>[] typeInfos;
    private RowTypeInfo rowTypeInfo;
    private List<String> columnNames;
    private Set<String> columnFamilies;
    private transient DeserializationFormat<byte[][], org.apache.flink.types.Row> deserializationFormat;
    private transient DeserializationSchema<byte[][], org.apache.flink.types.Row> deserializationSchema;

    /**
     * Parameters for Hbase/TableInputFormat.
     */
    private Map<String, Object> hbaseConfig;

    /**
     * Number of regions, used for computing parallelism.
     */
    private int regionCount;

    private static final Retryer<Object> RETRYER = RetryerBuilder.newBuilder()
            .retryIfException()
            .withWaitStrategy(WaitStrategies.fixedWait(Constants.RETRY_DELAY, TimeUnit.MILLISECONDS))
            .withStopStrategy(StopStrategies.stopAfterAttempt(Constants.RETRY_TIMES))
            .build();

    public HBaseSourceReader(BitSailConfiguration jobConf, SourceReader.Context readerContext, int subTaskId) {
        this.subTaskId = subTaskId;

        this.hbaseConfig = jobConf.get(HBaseReaderOptions.HBASE_CONF);
        this.tableName = jobConf.get(HBaseReaderOptions.TABLE);
        this.columnFamilies = new LinkedHashSet<>();
        this.typeInfos = readerContext.getTypeInfos();
        List<ColumnInfo> columnInfos = jobConf.getNecessaryOption(
                HBaseReaderOptions.COLUMNS, HBasePluginErrorCode.REQUIRED_VALUE);
        //typeInfos = TypeInfoUtils.getTypeInfos(createTypeInfoConverter(), columnInfos);
        this.columnNames = columnInfos.stream().map(ColumnInfo::getName).collect(Collectors.toList());

        // Get region numbers in hbase.
        this.regionCount = (int) RETRYER.call(() -> {
            Connection conn = HBaseHelper.getHbaseConnection(hbaseConfig);
            return conn.getAdmin().getRegions(TableName.valueOf(tableName)).size();
        });
        LOG.info("Got HBase region count {} to set Flink parallelism", regionCount);

        // Check if input column names are in format: [ columnFamily:column ].
        this.columnNames.stream().peek(column -> Preconditions.checkArgument(
                        (column.contains(":") && column.split(":").length == 2) || ROW_KEY.equalsIgnoreCase(column),
                        "Invalid column names, it should be [ColumnFamily:Column] format"))
                .forEach(column -> columnFamilies.add(column.split(":")[0]));
        // this.rowTypeInfo = ColumnFlinkTypeInfoUtil.getRowTypeInformation(createTypeInfoConverter(), columnInfos);
        LOG.info("HBase source reader {} is initialized.", subTaskId);

        deserializationFormat = new HBaseDeserializationFormat(jobConf);
        deserializationSchema = deserializationFormat.createRuntimeDeserializationSchema(typeInfos);
    }

    @Override
    public void start() {
        tableInputFormat = new TableInputFormat();
        tableInputFormat.setConf(getConf());
        namesMap = Maps.newConcurrentMap();
        LOG.info("Starting config HBase input format, maybe so slow........");
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

    public JobConf getConf() {
        JobConf jobConf = new JobConf(false);
        jobConf.set(TableInputFormat.INPUT_TABLE, tableName);
        hbaseConfig.forEach((key, value) -> jobConf.set(key, value.toString()));
        return jobConf;
    }
}
