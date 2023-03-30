/*
 * Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bytedance.bitsail.connector.hbase.sink;

import com.bytedance.bitsail.base.connector.writer.v1.Writer;
import com.bytedance.bitsail.base.connector.writer.v1.state.EmptyState;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.connector.hbase.HBaseHelper;
import com.bytedance.bitsail.connector.hbase.auth.KerberosAuthenticator;
import com.bytedance.bitsail.connector.hbase.constant.NullMode;
import com.bytedance.bitsail.connector.hbase.error.HBasePluginErrorCode;
import com.bytedance.bitsail.connector.hbase.option.HBaseWriterOptions;
import com.bytedance.bitsail.connector.hbase.sink.function.FunctionParser;
import com.bytedance.bitsail.connector.hbase.sink.function.FunctionTree;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedAction;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HBaseWriter<CommitT> implements Writer<Row, CommitT, EmptyState> {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseWriter.class);

  private static final String COLUMN_INDEX_KEY = "index";
  private static final String COLUMN_VALUE_KEY = "value";
  protected String tableName;
  protected String encoding;
  protected NullMode nullMode;
  protected boolean walFlag;
  protected long writeBufferSize;
  protected List<String> columnNames;
  protected String rowKeyExpress;
  protected Integer versionColumnIndex;
  protected Long versionColumnValue;
  protected List<String> rowKeyColumns = Lists.newArrayList();
  protected List<Integer> rowKeyColumnIndex = Lists.newArrayList();
  private Map<String, Object> hbaseConf;
  private boolean openUserKerberos = false;
  private boolean enableKerberos = false;
  private transient Connection connection;
  private transient BufferedMutator bufferedMutator;
  private transient FunctionTree functionTree;
  private transient Map<String, String[]> nameMaps;
  private transient Map<String, byte[][]> nameByteMaps;
  private transient ThreadLocal<SimpleDateFormat> timeSecondFormatThreadLocal;
  private transient ThreadLocal<SimpleDateFormat> timeMillisecondFormatThreadLocal;

  private static final long SLEEP_RANDOM_TIME_BASE = 5000L;
  private static final long SLEEP_RANDOM_TIME_RANGE = 10000;
  private static final int ROW_KEY_SIZE_DIV = 3;

  /**
   * Initiate DruidWriter with BitSailConfiguration.
   */
  public HBaseWriter(final BitSailConfiguration writerConfig, final Context<EmptyState> context) throws IOException {
    try {
      init(writerConfig, context);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    if (enableKerberos) {
      sleepRandomTime();
      UserGroupInformation ugi;
      if (openUserKerberos) {
        ugi = KerberosAuthenticator.getUgi(hbaseConf);
      } else {
        ugi = UserGroupInformation.getCurrentUser();
      }
      ugi.doAs((PrivilegedAction<Object>) () -> {
        openConnection();
        return null;
      });
    } else {
      openConnection();
    }
  }

  public void openConnection() {
    LOG.info("Start configuring ...");
    nameMaps = Maps.newConcurrentMap();
    nameByteMaps = Maps.newConcurrentMap();
    timeSecondFormatThreadLocal = new ThreadLocal();
    timeMillisecondFormatThreadLocal = new ThreadLocal();

    try {
      HBaseHelper hbasehelper = new HBaseHelper();
      org.apache.hadoop.conf.Configuration hbaseConfiguration = hbasehelper.getConfig(hbaseConf);
      connection = ConnectionFactory.createConnection(hbaseConfiguration);

      bufferedMutator = connection.getBufferedMutator(
          new BufferedMutatorParams(TableName.valueOf(tableName))
              .pool(HTable.getDefaultExecutor(hbaseConfiguration))
              .writeBufferSize(writeBufferSize));
    } catch (Exception e) {
      HBaseHelper hbasehelper = new HBaseHelper();
      hbasehelper.closeBufferedMutator(bufferedMutator);
      hbasehelper.closeConnection(connection);
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

    LOG.info("End configuring.");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(final Row element) {
    int i = 0;
    try {
      byte[] rowkey = getRowkey(element);
      Put put;
      if (versionColumnIndex == null) {
        put = new Put(rowkey);
        if (!walFlag) {
          put.setDurability(Durability.SKIP_WAL);
        }
      } else {
        long timestamp = getVersion(element);
        put = new Put(rowkey, timestamp);
      }

      for (; i < element.getArity(); ++i) {
        if (rowKeyColumnIndex.contains(i)) {
          continue;
        }

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
            throw new IllegalArgumentException("Wrong column format. Please make sure the column name is: " +
                "[columnFamily:columnName]. Wrong column is: " + name);
          }
        }
        byte[] columnBytes = getColumnByte(element.getField(i));
        if (null != columnBytes) {
          put.addColumn(
              cfAndQualifierBytes[0],
              cfAndQualifierBytes[1],
              columnBytes);
        }
      }
      bufferedMutator.mutate(put);

    } catch (Exception ex) {
      if (i < element.getArity()) {
        throw BitSailException.asBitSailException(CommonErrorCode.RUNTIME_ERROR,
            new RuntimeException(recordConvertDetailErrorMessage(i, element), ex));
      }
      throw BitSailException.asBitSailException(CommonErrorCode.RUNTIME_ERROR, ex);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flush(final boolean endOfInput) throws IOException {
    try {
      bufferedMutator.flush();
      LOG.info("HBase writer flush is done.");
    } catch (Exception e) {
      throw BitSailException.asBitSailException(CommonErrorCode.RUNTIME_ERROR, e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<CommitT> prepareCommit() {
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() throws IOException {
    if (null != timeSecondFormatThreadLocal) {
      timeSecondFormatThreadLocal.remove();
    }

    if (null != timeMillisecondFormatThreadLocal) {
      timeMillisecondFormatThreadLocal.remove();
    }

    HBaseHelper hbasehelper = new HBaseHelper();
    hbasehelper.closeBufferedMutator(bufferedMutator);
    hbasehelper.closeConnection(connection);
  }

  protected String recordConvertDetailErrorMessage(int pos, Row row) {
    return "HBaseWriter writeRecord error: when converting field[" + columnNames.get(pos) + "] in Row(" + row + ")";
  }

  private void init(final BitSailConfiguration writerConfig, final Context<EmptyState> context) throws Exception {
    tableName = writerConfig.getNecessaryOption(HBaseWriterOptions.TABLE_NAME, HBasePluginErrorCode.REQUIRED_VALUE);
    encoding = writerConfig.get(HBaseWriterOptions.ENCODING);
    nullMode = NullMode.valueOf(writerConfig.get(HBaseWriterOptions.NULL_MODE).toUpperCase());
    walFlag = writerConfig.get(HBaseWriterOptions.WAL_FLAG);
    writeBufferSize = writerConfig.get(HBaseWriterOptions.WRITE_BUFFER_SIZE);
    rowKeyExpress = buildRowKeyExpress(writerConfig.get(HBaseWriterOptions.ROW_KEY_COLUMN));
    Map<String, Object> versionColumn = writerConfig.get(HBaseWriterOptions.VERSION_COLUMN);
    if (versionColumn != null) {
      if (versionColumn.get(COLUMN_INDEX_KEY) != null) {
        versionColumnIndex = Integer.valueOf(versionColumn.get(COLUMN_INDEX_KEY).toString());
      } else if (versionColumn.get(COLUMN_VALUE_KEY) != null) {
        versionColumnValue = Long.valueOf(versionColumn.get(COLUMN_VALUE_KEY).toString());
        if (versionColumnValue < 0) {
          throw BitSailException.asBitSailException(HBasePluginErrorCode.ILLEGAL_VALUE,
              "Illegal timestamp to construct versionClumn: " + versionColumnValue);
        }
      }
    }

    hbaseConf = writerConfig.getNecessaryOption(HBaseWriterOptions.HBASE_CONF, HBasePluginErrorCode.REQUIRED_VALUE);

    boolean openPlatformKerberos = KerberosAuthenticator.enableKerberosConfig();
    openUserKerberos = KerberosAuthenticator.openKerberos(hbaseConf);
    enableKerberos = openPlatformKerberos || openUserKerberos;
    if (openPlatformKerberos && !openUserKerberos) {
      KerberosAuthenticator.addHBaseKerberosConf(hbaseConf);
    }
    columnNames = Lists.newArrayList(context.getRowTypeInfo().getFieldNames());
  }

  private void sleepRandomTime() {
    try {
      Thread.sleep(SLEEP_RANDOM_TIME_BASE + (long) (SLEEP_RANDOM_TIME_RANGE * Math.random()));
    } catch (Exception exception) {
      LOG.warn("", exception);
    }
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
        int index = Integer.parseInt(String.valueOf(indexObj));
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

  private byte[] getRowkey(Row record) throws Exception {
    Map<String, Object> nameValueMap = new HashMap<>((rowKeyColumnIndex.size() << 2) / ROW_KEY_SIZE_DIV);
    for (Integer keyColumnIndex : rowKeyColumnIndex) {
      nameValueMap.put(columnNames.get(keyColumnIndex), record.getField(keyColumnIndex));
    }
    String rowKeyStr = functionTree.evaluate(nameValueMap);
    return rowKeyStr.getBytes(StandardCharsets.UTF_8);
  }

  public long getVersion(Row record) {
    Integer index = versionColumnIndex.intValue();
    long timestamp;
    if (index == null) {
      timestamp = Long.valueOf(versionColumnValue);
      if (timestamp < 0) {
        throw new IllegalArgumentException("Illegal timestamp to construct versionClumn: " + timestamp);
      }
    } else {
      if (index >= record.getArity() || index < 0) {
        throw new IllegalArgumentException("version column index out of range: " + index);
      }
      if (record.getField(index) == null) {
        throw new IllegalArgumentException("null verison column!");
      }
      timestamp = (long) record.getField(index);
    }
    return timestamp;
  }

  public byte[] getColumnByte(Object column) {
    byte[] bytes;
    if (column != null) {
      bytes = Bytes.toBytes(column.toString());
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

}
