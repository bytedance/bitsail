/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bytedance.bitsail.connector.hbase.sink;

import com.bytedance.bitsail.base.enumerate.NullMode;
import com.bytedance.bitsail.flink.core.legacy.connector.OutputFormatPlugin;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedAction;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class HBaseOutputFormat extends OutputFormatPlugin<Row> implements ResultTypeQueryable<Row> {

  public static final String TIME_SECOND_SUFFIX = "sss";
  private static final String COLUMN_INDEX_KEY = "index";
  private static final String COLUMN_VALUE_KEY = "value";

  protected String tableName;

  protected String encoding;

  protected NullMode nullMode;

  protected boolean walFlag;

  protected long writeBufferSize;

  protected List<String> columnTypes;

  protected List<String> columnNames;

  protected String rowKeyExpress;

  protected Integer versionColumnIndex;

  protected Long versionColumnValue;

  protected List<String> rowKeyColumns = Lists.newArrayList();

  protected List<Integer> rowKeyColumnIndex = Lists.newArrayList();

  private Map<String, Object> hBaseConf;

  private RowTypeInfo rowTypeInfo;

  private boolean openUserKerberos = false;

  private boolean enableKerberos = false;

  private transient Connection connection;

  private transient BufferedMutator bufferedMutator;

  private transient FunctionTree functionTree;

  private transient Map<String, String[]> nameMaps;

  private transient Map<String, byte[][]> nameByteMaps;

  private transient ThreadLocal<SimpleDateFormat> timeSecondFormatThreadLocal;

  private transient ThreadLocal<SimpleDateFormat> timeMillisecondFormatThreadLocal;

  /**
   * Connects to the target database and initializes the prepared statement.
   *
   * @param taskNumber The number of the parallel instance.
   *                   I/O problem.
   */
  @Override
  public void open(int taskNumber, int numTasks) throws IOException {
    super.open(taskNumber, numTasks);
    if (enableKerberos) {
      sleepRandomTime();
      UserGroupInformation ugi;
      if (openUserKerberos) {
        //底座未开启鉴权，使用用户上传鉴权文件初始化ugi
        ugi = HBaseHelper.getUgi(hBaseConf);
      } else {
        //共用底座ugi，Flink启动TM时已完成ugi鉴权初始化
        ugi = HBaseHelper.getLoginUser();
      }
      ugi.doAs(new PrivilegedAction<Object>() {
        @Override
        public Object run() {
          openConnection();
          return null;
        }
      });
    } else {
      openConnection();
    }
  }

  private void sleepRandomTime() {
    try {
      Thread.sleep(5000L + (long) (10000 * Math.random()));
    } catch (Exception exception) {
      log.warn("", exception);
    }
  }

  public void openConnection() {
    log.info("HbaseOutputFormat configure start");
    nameMaps = Maps.newConcurrentMap();
    nameByteMaps = Maps.newConcurrentMap();
    timeSecondFormatThreadLocal = new ThreadLocal();
    timeMillisecondFormatThreadLocal = new ThreadLocal();

    try {
      org.apache.hadoop.conf.Configuration hConfiguration = HBaseHelper.getConfig(hBaseConf);
      connection = ConnectionFactory.createConnection(hConfiguration);

      bufferedMutator = connection.getBufferedMutator(
          new BufferedMutatorParams(TableName.valueOf(tableName))
              .pool(HTable.getDefaultExecutor(hConfiguration))
              .writeBufferSize(writeBufferSize));
    } catch (Exception e) {
      HBaseHelper.closeBufferedMutator(bufferedMutator);
      HBaseHelper.closeConnection(connection);
      throw new IllegalArgumentException(e);
    }

    functionTree = FunctionParser.parse(rowKeyExpress);
    rowKeyColumns = FunctionParser.parseRowKeyCol(rowKeyExpress);
    for (String rowKeyColumn : rowKeyColumns) {
      int index = columnNames.indexOf(rowKeyColumn);
      if (index == -1) {
        throw new RuntimeException("Can not get row key column from columns:" + rowKeyColumn);
      }
      rowKeyColumnIndex.add(index);
    }

    log.info("HbaseOutputFormat configure end");
  }

  @Override
  public void writeRecordInternal(Row record) throws Exception {
    int i = 0;
    try {
      byte[] rowkey = getRowkey(record);
      Put put;
      if (versionColumnIndex == null) {
        put = new Put(rowkey);
        if (!walFlag) {
          put.setDurability(Durability.SKIP_WAL);
        }
      } else {
        long timestamp = getVersion(record);
        put = new Put(rowkey, timestamp);
      }

      for (; i < record.getArity(); ++i) {
        if (rowKeyColumnIndex.contains(i)) {
          continue;
        }

        String type = columnTypes.get(i);
        String name = columnNames.get(i);
        String[] cfAndQualifier = nameMaps.get(name);
        byte[][] cfAndQualifierBytes = nameByteMaps.get(name);
        if (cfAndQualifier == null || cfAndQualifierBytes == null) {
          cfAndQualifier = name.split(":");
          if (cfAndQualifier.length == 2
              && StringUtils.isNotBlank(cfAndQualifier[0])
              && StringUtils.isNotBlank(cfAndQualifier[1])) {
            nameMaps.put(name, cfAndQualifier);
            cfAndQualifierBytes = new byte[2][];
            cfAndQualifierBytes[0] = Bytes.toBytes(cfAndQualifier[0]);
            cfAndQualifierBytes[1] = Bytes.toBytes(cfAndQualifier[1]);
            nameByteMaps.put(name, cfAndQualifierBytes);
          } else {
            throw new IllegalArgumentException("Hbasewriter 中，column 的列配置格式应该是：列族:列名. 您配置的列错误：" + name);
          }
        }
        byte[] columnBytes = getColumnByte(type, (Column) record.getField(i));
        //columnBytes 为null忽略这列
        if (null != columnBytes) {
          put.addColumn(
              cfAndQualifierBytes[0],
              cfAndQualifierBytes[1],
              columnBytes);
        }
      }

      bufferedMutator.mutate(put);
    } catch (Exception ex) {
      if (i < record.getArity()) {
        throw DTSException.asDTSException(CommonErrorCode.RUNTIME_ERROR, new RuntimeException(recordConvertDetailErrorMessage(i, record), ex));
      }
      throw DTSException.asDTSException(CommonErrorCode.RUNTIME_ERROR, ex);
    }
  }

  protected String recordConvertDetailErrorMessage(int pos, Row row) {
    return "HbaseOutputFormat writeRecord error: when converting field[" + columnNames.get(pos) + "] in Row(" + row + ")";
  }

  private SimpleDateFormat getSimpleDateFormat(String sign) {
    SimpleDateFormat format;
    if (TIME_SECOND_SUFFIX.equals(sign)) {
      format = timeSecondFormatThreadLocal.get();
      if (format == null) {
        format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        timeSecondFormatThreadLocal.set(format);
      }
    } else {
      format = timeMillisecondFormatThreadLocal.get();
      if (format == null) {
        format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");
        timeMillisecondFormatThreadLocal.set(format);
      }
    }

    return format;
  }

  private byte[] getRowkey(Row record) throws Exception {
    Map<String, Object> nameValueMap = new HashMap<>((rowKeyColumnIndex.size() << 2) / 3);
    for (Integer keyColumnIndex : rowKeyColumnIndex) {
      nameValueMap.put(columnNames.get(keyColumnIndex), ((Column) record.getField(keyColumnIndex)).asString());
    }
    String rowKeyStr = functionTree.evaluate(nameValueMap);
    return rowKeyStr.getBytes(StandardCharsets.UTF_8);
  }

  public long getVersion(Row record) {
    Integer index = versionColumnIndex.intValue();
    long timestamp;
    if (index == null) {
      //指定时间作为版本
      timestamp = Long.valueOf(versionColumnValue);
      if (timestamp < 0) {
        throw new IllegalArgumentException("Illegal timestamp to construct versionClumn: " + timestamp);
      }
    } else {
      //指定列作为版本,long/doubleColumn直接record.aslong, 其它类型尝试用yyyy-MM-dd HH:mm:ss,yyyy-MM-dd HH:mm:ss SSS去format
      if (index >= record.getArity() || index < 0) {
        throw new IllegalArgumentException("version column index out of range: " + index);
      }
      if (record.getField(index) == null) {
        throw new IllegalArgumentException("null verison column!");
      }
      Column col = (Column) record.getField(index);
      timestamp = col.asLong();
    }
    return timestamp;
  }

  public byte[] getColumnByte(String columnType, Column column) {
    byte[] bytes;
    if (column != null && column.getRawData() != null) {
      switch (columnType) {
        case "int":
          bytes = Bytes.toBytes((int) (long) column.asLong());
          break;
        case "long":
        case "bigint":
          bytes = Bytes.toBytes(column.asLong());
          break;
        case "date":
        case "timestamp":
          bytes = Bytes.toBytes(column.asLong());
          break;
        case "double":
          bytes = Bytes.toBytes(column.asDouble());
          break;
        case "float":
          bytes = Bytes.toBytes((float) (double) column.asDouble());
          break;
        case "decimal":
          bytes = Bytes.toBytes(column.asBigDecimal());
          break;
        case "short":
          bytes = Bytes.toBytes((short) (long) column.asLong());
          break;
        case "boolean":
          bytes = Bytes.toBytes(column.asBoolean());
          break;
        case "varchar":
        case "string":
          bytes = Bytes.toBytes(column.asString());
          break;
        case "binary":
          bytes = column.asBytes();
          break;
        default:
          throw new IllegalArgumentException("Unsupported column type: " + columnType);
      }
    } else {
      switch (nullMode) {
        case SKIP:
          bytes = null;
          break;
        case EMPTY:
          bytes = HConstants.EMPTY_BYTE_ARRAY;
          break;
        default:
          throw new IllegalArgumentException("Unsupported null mode: " + nullMode);
      }
    }
    return bytes;
  }

  @Override
  public TypeSystem getTypeSystem() {
    return TypeSystem.DTS;
  }

  @Override
  public void close() throws IOException {
    super.close();
    if (null != timeSecondFormatThreadLocal) {
      timeSecondFormatThreadLocal.remove();
    }

    if (null != timeMillisecondFormatThreadLocal) {
      timeMillisecondFormatThreadLocal.remove();
    }

    HBaseHelper.closeBufferedMutator(bufferedMutator);
    HBaseHelper.closeConnection(connection);
  }

  @Override
  public int getMaxParallelism() {
    return MAX_PARALLELISM_OUTPUT_HBASE;
  }

  @Override
  public void initPlugin() throws Exception {
    tableName = outputSliceConfig.getNecessaryOption(WriterOptions.HBaseOutputOptions.TABLE_NAME, HBasePluginErrorCode.REQUIRED_VALUE);
    encoding = outputSliceConfig.get(WriterOptions.HBaseOutputOptions.ENCODING);
    nullMode = NullMode.valueOf(outputSliceConfig.get(WriterOptions.HBaseOutputOptions.NULL_MODE).toUpperCase());
    walFlag = outputSliceConfig.get(WriterOptions.HBaseOutputOptions.WAL_FLAG);
    writeBufferSize = outputSliceConfig.get(WriterOptions.HBaseOutputOptions.WRITE_BUFFER_SIZE);
    rowKeyExpress = buildRowKeyExpress(outputSliceConfig.get(WriterOptions.HBaseOutputOptions.ROW_KEY_COLUMN));
    Map<String, Object> versionColumn = outputSliceConfig.get(WriterOptions.HBaseOutputOptions.VERSION_COLUMN);
    if (versionColumn != null) {
      if (versionColumn.get(COLUMN_INDEX_KEY) != null) {
        versionColumnIndex = Integer.valueOf(versionColumn.get(COLUMN_INDEX_KEY).toString());
      } else if (versionColumn.get(COLUMN_VALUE_KEY) != null) {
        versionColumnValue = Long.valueOf(versionColumn.get(COLUMN_VALUE_KEY).toString());
        if (versionColumnValue < 0) {
          throw DTSException.asDTSException(HBasePluginErrorCode.ILLEGAL_VALUE, "Illegal timestamp to construct versionClumn: " + versionColumnValue);
        }
      }
    }

    hBaseConf = outputSliceConfig.getNecessaryOption(WriterOptions.HBaseOutputOptions.HBASE_CONF, HBasePluginErrorCode.REQUIRED_VALUE);

    boolean openPlatformKerberos = Tools.enableKerberosConfig();
    openUserKerberos = HBaseHelper.openKerberos(hBaseConf);
    enableKerberos = openPlatformKerberos || openUserKerberos;
    if (openPlatformKerberos && !openUserKerberos) {
      //若集群底座开启kerberos鉴权时，添加hbase相关鉴权参数
      HBaseHelper.addHbaseKerberosConf(hBaseConf);
    }

    List<ColumnInfo> columns = outputSliceConfig.getNecessaryOption(WriterOptions.HBaseOutputOptions.COLUMNS, HBasePluginErrorCode.REQUIRED_VALUE);
    columnNames = columns.stream().map(ColumnInfo::getName).collect(Collectors.toList());
    columnTypes = columns.stream().map(ColumnInfo::getType).collect(Collectors.toList());
    rowTypeInfo = NativeFlinkTypeInfoUtil.getRowTypeInformation("hbase", columns);
  }

  @Override
  public String getType() {
    return "HBase";
  }

  @Override
  public void tryCleanupOnError() throws Exception {

  }

  @Override
  public RowTypeInfo getProducedType() {
    return rowTypeInfo;
  }

  /**
   * Compatible with old formats
   */
  private String buildRowKeyExpress(Object rowKeyInfo) {
    if (rowKeyInfo == null) {
      return null;
    }

    if (rowKeyInfo instanceof String) {
      return rowKeyInfo.toString();
    }

    if (!(rowKeyInfo instanceof List)) {
      return null;
    }

    StringBuilder expressBuilder = new StringBuilder();

    for (Map item : ((List<Map>) rowKeyInfo)) {
      Object indexObj = item.get(COLUMN_INDEX_KEY);
      if (indexObj != null) {
        Integer index = Integer.valueOf(String.valueOf(indexObj));
        if (index >= 0) {
          expressBuilder.append(String.format("$(%s)", columnNames.get(index)));
          continue;
        }
      }

      String value = (String) item.get(COLUMN_VALUE_KEY);
      if (StringUtils.isNotEmpty(value)) {
        expressBuilder.append(value);
      }
    }

    return expressBuilder.toString();
  }

}
