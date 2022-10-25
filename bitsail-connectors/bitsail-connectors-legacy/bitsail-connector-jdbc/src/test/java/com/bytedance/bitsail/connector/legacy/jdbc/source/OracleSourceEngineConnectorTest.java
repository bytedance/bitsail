package com.bytedance.bitsail.connector.legacy.jdbc.source;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.ddl.typeinfo.PrimitiveTypes;
import com.bytedance.bitsail.common.ddl.typeinfo.TypeInfo;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.type.BaseEngineTypeInfoConverter;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created 2022/10/18
 */
public class OracleSourceEngineConnectorTest {
  private final String json = "{\n" +
          "  \"job\":{\n" +
          "    \"reader\":{\n" +
          "      \"password\":\"test\",\n" +
          "      \"db_name\":\"GL\",\n" +
          "      \"user_name\":\"test\",\n" +
          "      \"table_name\":\"GL.GL_JE_HEADERS\",\n" +
          "      \"class\":\"com.bytedance.bitsail.connector.legacy.jdbc.source.OracleInputFormat\",\n" +
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
    columnInfoList.add(new ColumnInfo("COL1", "varchar"));
    columnInfoList.add(new ColumnInfo("COL2", "number"));
    columnInfoList.add(new ColumnInfo("COL3", "integer"));
    columnInfoList.add(new ColumnInfo("COL4", "int"));
    columnInfoList.add(new ColumnInfo("COL5", "smallint"));
    columnInfoList.add(new ColumnInfo("COL6", "float"));
    columnInfoList.add(new ColumnInfo("COL7", "double"));
    columnInfoList.add(new ColumnInfo("COL8", "decimal"));
    columnInfoList.add(new ColumnInfo("COL9", "bool"));
    columnInfoList.add(new ColumnInfo("COL10", "date"));
    columnInfoList.add(new ColumnInfo("COL11", "timestamp"));
    columnInfoList.add(new ColumnInfo("COL12", "blob"));
    BitSailConfiguration conf = BitSailConfiguration.from(json);
    OracleSourceEngineConnector oracleSourceExternalEngineConnector = new OracleSourceEngineConnector(conf, conf);

    BaseEngineTypeInfoConverter typeInfoConverter = oracleSourceExternalEngineConnector.createTypeInfoConverter();

    Map<String, TypeInfo<?>> actualTypeInfoMap = Maps.newLinkedHashMap();
    for (ColumnInfo columnInfo : columnInfoList) {
      actualTypeInfoMap.put(columnInfo.getName(), typeInfoConverter.toTypeInfo(columnInfo.getType()));
    }

    LinkedHashMap<String, TypeInfo<?>> expectTypeInfoMap = new LinkedHashMap<>();
    expectTypeInfoMap.put("COL1", PrimitiveTypes.STRING.getTypeInfo());
    expectTypeInfoMap.put("COL2", PrimitiveTypes.BIGINT.getTypeInfo());
    expectTypeInfoMap.put("COL3", PrimitiveTypes.INT.getTypeInfo());
    expectTypeInfoMap.put("COL4", PrimitiveTypes.INT.getTypeInfo());
    expectTypeInfoMap.put("COL5", PrimitiveTypes.SHORT.getTypeInfo());
    expectTypeInfoMap.put("COL6", PrimitiveTypes.DOUBLE.getTypeInfo());
    expectTypeInfoMap.put("COL7", PrimitiveTypes.DOUBLE.getTypeInfo());
    expectTypeInfoMap.put("COL8", PrimitiveTypes.BIG_DECIMAL.getTypeInfo());
    expectTypeInfoMap.put("COL9", PrimitiveTypes.BOOLEAN.getTypeInfo());
    expectTypeInfoMap.put("COL10", PrimitiveTypes.DATE_DATE.getTypeInfo());
    expectTypeInfoMap.put("COL11", PrimitiveTypes.DATE_DATE_TIME.getTypeInfo());
    expectTypeInfoMap.put("COL12", PrimitiveTypes.BINARY.getTypeInfo());
    assertEquals(actualTypeInfoMap.toString(), expectTypeInfoMap.toString());
  }
}
