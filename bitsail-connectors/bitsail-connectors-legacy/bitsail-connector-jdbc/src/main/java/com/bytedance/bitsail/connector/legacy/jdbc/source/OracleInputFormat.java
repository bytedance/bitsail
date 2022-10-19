package com.bytedance.bitsail.connector.legacy.jdbc.source;

import com.bytedance.dts.batch.jdbc.converter.JdbcValueConverter;
import com.bytedance.dts.batch.jdbc.converter.OracleValueConverter;
import com.bytedance.dts.common.configuration.DtsConfiguration;
import com.bytedance.dts.common.configuration.ReaderOptions;
import com.bytedance.dts.common.ddl.source.SourceEngineConnector;
import com.bytedance.dts.common.model.ColumnInfo;
import com.bytedance.dts.common.util.OracleUtil;
import com.bytedance.dts.common.util.TypeConvertUtil.StorageEngine;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.ExecutionException;

@Slf4j
public class OracleInputFormat extends JDBCInputFormat {
  private static final long serialVersionUID = 1L;
  private String intervalHandlingMode;

  public OracleInputFormat() {
  }

  @Override
  public void initPlugin() throws ExecutionException, InterruptedException {
    super.initPlugin();
    this.intervalHandlingMode = inputSliceConfig.getUnNecessaryOption(ReaderOptions.OracleReaderOptions.INTERVAL_HANDLE_MODE, ReaderOptions.OracleReaderOptions.INTERVAL_HANDLE_MODE.defaultValue());
  }

  @Override
  public String getType() {
    return "Oracle";
  }

  @Override
  String genSqlTemplate(String splitPK, List<ColumnInfo> columns, String filter) {
    StringBuilder sql = new StringBuilder("SELECT ");
    final String tableAlias = "t";
    for (ColumnInfo column : columns) {
      String columnName = column.getName();
      String columnType = column.getType();
      // xmltype 当作 clob 类型处理
      // sql 语句类似 select t."DOC".getClobVal() as "DOC", "TEST" from po_xml_tab t
      // 详见 Oracle xmltype 文档 https://docs.oracle.com/cd/B14117_01/appdev.101/b10790/xdb11jav.htm
      if ("xmltype".equalsIgnoreCase(columnType)) {
        sql.append(tableAlias).append(".").append(getQuoteColumn(columnName.toUpperCase())).append(".getClobVal() as ")
          .append(getQuoteColumn(columnName.toUpperCase())).append(",");
      } else {
        sql.append(getQuoteColumn(columnName.toUpperCase())).append(",");
      }
    }
    sql.deleteCharAt(sql.length() - 1);
    sql.append(" FROM ").append("%s").append(" ").append(tableAlias).append(" WHERE ");
    if (null != filter) {
      sql.append("(").append(filter).append(")");
      sql.append(" AND ");
    }
    sql.append("(").append(splitPK).append(" >= ? AND ").append(splitPK).append(" < ?)");
    return sql.toString();
  }

  @Override
  public String getDriverName() {
    return OracleUtil.DRIVER_NAME;
  }

  @Override
  public String getFieldQuote() {
    return OracleUtil.DB_QUOTE;
  }

  @Override
  public String getValueQuote() {
    return OracleUtil.VALUE_QUOTE;
  }

  @Override
  public StorageEngine getStorageEngine() {
    return StorageEngine.oracle;
  }

  @Override
  public SourceEngineConnector initSourceSchemaManager(DtsConfiguration commonConf, DtsConfiguration readerConf) throws Exception {
    return new OracleSourceSchemaManagerGenerator().genJdbcSchemaManager(commonConf, readerConf);
  }

  @Override
  protected JdbcValueConverter createJdbcValueConverter() {
    return new OracleValueConverter(OracleValueConverter.IntervalHandlingMode.parse(this.intervalHandlingMode));
  }
}
