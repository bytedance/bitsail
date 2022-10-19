package com.bytedance.bitsail.connector.legacy.jdbc.source;

import com.bytedance.dts.common.DTSException;
import com.bytedance.dts.common.configuration.DtsConfiguration;
import com.bytedance.dts.common.configuration.ReaderOptions;
import com.bytedance.dts.common.exception.CommonErrorCode;
import com.bytedance.dts.common.model.ColumnInfo;
import com.bytedance.dts.common.model.TableInfo;
import com.bytedance.dts.common.tools.jdbc.ClusterInfo;
import com.bytedance.dts.common.tools.jdbc.ConnectionInfo;
import com.bytedance.dts.common.type.BaseEngineTypeInfoConverter;
import com.bytedance.dts.common.type.JdbcTypeInfoConverter;
import com.bytedance.dts.common.util.JsonUtils;
import com.bytedance.dts.common.util.OracleUtil;
import com.google.common.base.Strings;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;

public class OracleSourceEngineConnector extends SourceEngineConnector {
  private String url;
  private String schemaName;
  private String tableName;
  private String userName;
  private String password;
  private String initSql;

  public OracleSourceEngineConnector(DtsConfiguration commonConfiguration,
                                     DtsConfiguration readerConfiguration) {
    super(commonConfiguration, readerConfiguration);
    List<ClusterInfo> clusterInfos = readerConfiguration.get(ReaderOptions.JdbcReaderOptions.CONNECTIONS);
    List<ConnectionInfo> slaves = clusterInfos.get(0).getSlaves();
    if (CollectionUtils.isEmpty(slaves)) {
      throw DTSException.asDTSException(CommonErrorCode.CONFIG_ERROR,
        "Get DB information error, the slaves info is null! Connection Json string is " + JsonUtils.toJson(clusterInfos));
    }
    url = slaves.get(0).getUrl();
    schemaName = readerConfiguration.get(ReaderOptions.OracleReaderOptions.TABLE_SCHEMA);
    tableName = readerConfiguration.get(ReaderOptions.OracleReaderOptions.TABLE_NAME);
    userName = readerConfiguration.get(ReaderOptions.OracleReaderOptions.USER_NAME);
    password = readerConfiguration.get(ReaderOptions.OracleReaderOptions.PASSWORD);
    initSql = readerConfiguration.get(ReaderOptions.OracleReaderOptions.INIT_SQL);
  }

  @Override
  public List<ColumnInfo> getExternalColumnInfos() throws Exception {
    OracleUtil oracleUtil = new OracleUtil();
    TableInfo tableInfo;
    if (!Strings.isNullOrEmpty(tableName) && tableName.toUpperCase().split("\\.").length == 2) {
      //todo 兼容旧版本参数逻辑，表名格式为schema.table，完成Processor升级后可以去掉
      tableInfo = oracleUtil.getTableInfo(url, userName, password, tableName, initSql);
    } else {
      tableInfo = oracleUtil.getTableInfo(url, userName, password, null, schemaName, tableName, initSql, null);
    }
    return tableInfo.getColumnInfoList();
  }

  @Override
  public BaseEngineTypeInfoConverter createTypeInfoConverter() {
    return new JdbcTypeInfoConverter(getExternalEngineName());
  }


  @Override
  public String getExternalEngineName() {
    return "oracle";
  }
}
