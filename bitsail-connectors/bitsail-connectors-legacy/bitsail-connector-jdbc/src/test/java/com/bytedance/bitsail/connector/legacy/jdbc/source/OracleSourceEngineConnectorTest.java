package com.bytedance.bitsail.connector.legacy.jdbc.source;

import com.bytedance.dts.common.configuration.DtsConfiguration;
import com.bytedance.dts.common.ddl.typeinfo.PrimitiveTypes;
import com.bytedance.dts.common.ddl.typeinfo.TypeInfo;
import com.bytedance.dts.common.model.ColumnInfo;
import com.bytedance.dts.common.type.BaseEngineTypeInfoConverter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created 2022/5/11
 *
 * @author ke.hao
 */
public class OracleSourceEngineConnectorTest {
  private final String json = "{\n" +
    "  \"job\":{\n" +
    "    \"reader\":{\n" +
    "      \"password\":\"test\",\n" +
    "      \"db_name\":\"GL\",\n" +
    "      \"user_name\":\"test\",\n" +
    "      \"table_name\":\"GL.GL_JE_HEADERS\",\n" +
    "      \"class\":\"com.bytedance.dts.batch.jdbc.OracleInputFormat\",\n" +
    "      \"connections\":[\n" +
    "\n" +
    "        {\n" +
    "          \"slaves\":[\n" +
    "            {\n" +
    "              \"port\":1521,\n" +
    "              \"db_url\":\"jdbc:oracle:thin:@fakeip:1521/test3\",\n" +
    "              \"host\":\"10.92.3.16\"\n" +
    "            }\n" +
    "          ],\n" +
    "          \"shard_num\":0,\n" +
    "          \"master\":{\n" +
    "\n" +
    "            \"port\":1521,\n" +
    "            \"host\":\"10.92.3.16\",\n" +
    "            \"db_url\":\"jdbc:oracle:thin:@fakeip:1521/test3\"\n" +
    "          }\n" +
    "        }\n" +
    "      ]\n" +
    "    }" +
    "    }" +
    "    }";

  @Test
  public void getTypeInfoMapTest() {
    List<ColumnInfo> columnInfoList = Lists.newArrayList();
    columnInfoList.add(new ColumnInfo("col1", "varchar"));
    columnInfoList.add(new ColumnInfo("col2", "number"));
    columnInfoList.add(new ColumnInfo("col3", "integer"));
    columnInfoList.add(new ColumnInfo("col4", "int"));
    columnInfoList.add(new ColumnInfo("col5", "smallint"));
    columnInfoList.add(new ColumnInfo("col6", "float"));
    columnInfoList.add(new ColumnInfo("col7", "double"));
    columnInfoList.add(new ColumnInfo("col8", "decimal"));
    columnInfoList.add(new ColumnInfo("col9", "bool"));
    columnInfoList.add(new ColumnInfo("col10", "date"));
    columnInfoList.add(new ColumnInfo("col11", "timestamp"));
    columnInfoList.add(new ColumnInfo("col12", "blob"));
    DtsConfiguration conf = DtsConfiguration.from(json);
    OracleSourceEngineConnector oracleSourceExternalEngineConnector
      = new OracleSourceEngineConnector(conf, conf);

    BaseEngineTypeInfoConverter typeInfoConverter = oracleSourceExternalEngineConnector
      .createTypeInfoConverter();

    Map<String, TypeInfo<?>> actualTypeInfoMap = Maps.newLinkedHashMap();
    for (ColumnInfo columnInfo : columnInfoList) {
      actualTypeInfoMap.put(columnInfo.getName(), typeInfoConverter
        .toTypeInfo(columnInfo.getType()));
    }

    LinkedHashMap<String, TypeInfo<?>> expectTypeInfoMap = new LinkedHashMap<>();
    expectTypeInfoMap.put("col1", PrimitiveTypes.STRING.getTypeInfo());
    expectTypeInfoMap.put("col2", PrimitiveTypes.BIGINT.getTypeInfo());
    expectTypeInfoMap.put("col3", PrimitiveTypes.INT.getTypeInfo());
    expectTypeInfoMap.put("col4", PrimitiveTypes.INT.getTypeInfo());
    expectTypeInfoMap.put("col5", PrimitiveTypes.SHORT.getTypeInfo());
    expectTypeInfoMap.put("col6", PrimitiveTypes.DOUBLE.getTypeInfo());
    expectTypeInfoMap.put("col7", PrimitiveTypes.DOUBLE.getTypeInfo());
    expectTypeInfoMap.put("col8", PrimitiveTypes.BIG_DECIMAL.getTypeInfo());
    expectTypeInfoMap.put("col9", PrimitiveTypes.BOOLEAN.getTypeInfo());
    expectTypeInfoMap.put("col10", PrimitiveTypes.DATE_DATE.getTypeInfo());
    expectTypeInfoMap.put("col11", PrimitiveTypes.DATE_DATE_TIME.getTypeInfo());
    expectTypeInfoMap.put("col12", PrimitiveTypes.BINARY.getTypeInfo());
    assertEquals(actualTypeInfoMap.toString(), expectTypeInfoMap.toString());
  }
}
