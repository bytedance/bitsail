package com.bytedance.bitsail.connector.legacy.jdbc.source;

import com.bytedance.bitsail.connector.legacy.jdbc.converter.OracleValueConverter;

import oracle.jdbc.OracleResultSet;
import oracle.jdbc.OracleResultSetMetaData;
import oracle.jdbc.OracleTypes;
import oracle.sql.TIMESTAMP;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.Timestamp;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.when;

public class OracleInputFormatTest {

  @Test
  public void getSupportedTypeRowDataTest() throws Exception {
    int columnType = OracleTypes.TIMESTAMPTZ;
    TIMESTAMP timestamp = new TIMESTAMP(new Timestamp(1539492540));
    OracleResultSet resultSet = Mockito.mock(OracleResultSet.class);
    OracleResultSetMetaData metaData = Mockito.mock(OracleResultSetMetaData.class);
    when(resultSet.getTIMESTAMP(anyInt())).thenReturn(timestamp);
    when(metaData.getColumnType(anyInt())).thenReturn(columnType);

    OracleValueConverter oracleValueConverter = new OracleValueConverter(OracleValueConverter.IntervalHandlingMode.NUMERIC);
    Object convert = oracleValueConverter.convert(metaData, resultSet, 1, null);
    Assert.assertEquals(convert, timestamp.timestampValue());
  }
}
