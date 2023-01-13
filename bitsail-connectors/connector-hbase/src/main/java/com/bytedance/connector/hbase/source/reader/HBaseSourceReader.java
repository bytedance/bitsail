package com.bytedance.connector.hbase.source.reader;

import ch.qos.logback.classic.db.names.TableName;
import com.bytedance.bitsail.base.connector.reader.v1.SourceEvent;
import com.bytedance.bitsail.base.connector.reader.v1.SourcePipeline;
import com.bytedance.bitsail.base.connector.reader.v1.SourceReader;
import com.bytedance.bitsail.base.format.DeserializationFormat;
import com.bytedance.bitsail.base.format.DeserializationSchema;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfoUtils;
import com.bytedance.bitsail.common.util.Preconditions;
import com.bytedance.connector.hbase.HBaseHelper;
import com.bytedance.connector.hbase.error.HBasePluginErrorCode;
import com.bytedance.connector.hbase.option.HBaseReaderOptions;
import com.bytedance.connector.hbase.source.split.HBaseSourceSplit;

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.Collectors;

public class HBaseSourceReader implements SourceReader<Row, HBaseSourceSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseSourceReader.class);

    private final int subTaskId;

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

    public HBaseSourceReader(BitSailConfiguration jobConf, SourceReader.Context readerContext, int subTaskId) {
        this.subTaskId = subTaskId;

        this.hbaseConfig = jobConf.get(HBaseReaderOptions.HBASE_CONF);
        this.tableName = jobConf.get(HBaseReaderOptions.TABLE);
        this.columnFamilies = new LinkedHashSet<>();

        List<ColumnInfo> columnInfos = jobConf.getNecessaryOption(
                HBaseReaderOptions.COLUMNS, HBasePluginErrorCode.REQUIRED_VALUE);
        typeInfos = TypeInfoUtils.getTypeInfos(createTypeInfoConverter(), columnInfos);

        columnNames = columnInfos.stream().map(ColumnInfo::getName).collect(Collectors.toList());

        // Get region numbers in hbase.
        regionCount = (int) RETRYER.call(() -> {
            Connection hbaseClient = HBaseHelper.getHbaseConnection(hbaseConfig);
            return hbaseClient.getAdmin().getRegions(TableName.valueOf(tableName)).size();
        });
        LOG.info("Got HBase region count {} to set Flink parallelism", regionCount);

        // Check if input column names are in format: [ columnFamily:column ].
        columnNames.stream().peek(column -> Preconditions.checkArgument(
                        (column.contains(":") && column.split(":").length == 2) || ROW_KEY.equalsIgnoreCase(column),
                        "Invalid column names, it should be [ColumnFamily:Column] format"))
                .forEach(column -> columnFamilies.add(column.split(":")[0]));
        rowTypeInfo = ColumnFlinkTypeInfoUtil.getRowTypeInformation(createTypeInfoConverter(), columnInfos);
        LOG.info("HBase source reader {} is initialized.", subTaskId);
    }

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
