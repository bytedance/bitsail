# bitsail-component-format-flink-hive

-----

上级文档: [bitsail-component-format-flink](./introduction.md)

本模块中的`HiveGeneralRowBuilder`支持将从hive读出的原始`Writable`格式数据转化为`bitsail row`。

## 如何构造

`HiveGeneralRowBuilder`的工作原理是先从hive metastore中获取指定hive表的元信息，然后根据元信息中的`ObjectInspector`来解析数据。

因此在构造方法中需要两种参数：
 1. 用于获取hive元信息的参数
    - `database`: hive库名
    - `table`: hive表名
    - `hiveProperties`: 以Map形式存储的用于连接metastore的properties
 2. `columnMapping`: 构建的row中字段顺序，以Map<String,Integer>形式存储。key为row的最终字段名，value表示该字段在hive中的index。

### 构建示例

以下面的hive表 <span id="jump_example_table">`test_db.test_table`</span> 为例:
 - 元数据存储的thrift URI为 `thrift://localhost:9083`

| 字段名      | 字段类型     |
|----------|----------|
| `id`     | `BIGINT` |
| `state`  | `STRING` |
| `county` | `STRING` |

则可使用如下方法构造`HiveGeneralRowBuilder`:

```
Map<String, Integer> columnMapping = ImmutableMap.of(
  "id", 0,
  "state", 1,
  "county", 2
);

RowBuilder rowBuilder = new HiveGeneralRowBuilder(
  columnMapping,
  "test_db",
  "test_table",
  ImmutableMap.of("metastore_uri", "thrift://localhost:9083")
);
```

## <span id="jump_parse">如何解析Writable</span>

为了解析`Writable`数据，需要hive数据表中的`deserializer`信息和`ObjectInspector`等元信息。

本模块支持的HiveGeneralRowBuilder根据用户传入的hive信息（包括database、table、以及一些其他用于连接metastore的properties），获取到解析需要的元信息。

```
// step1. Get hive meta info from metastore.
HiveMetaClientUtil.init();
HiveConf hiveConf = HiveMetaClientUtil.getHiveConf(hiveProperties);
StorageDescriptor storageDescriptor = HiveMetaClientUtil.getTableFormat(hiveConf, db, table);

// step2. Construct deserializer.
deserializer = (Deserializer) Class.forName(storageDescriptor.getSerdeInfo().getSerializationLib()).newInstance();
SerDeUtils.initializeSerDe(deserializer, conf, properties, null);

// step3. Construct `ObjectInspector`
structObjectInspector = (StructObjectInspector) deserializer.getObjectInspector();
structFields = structObjectInspector.getAllStructFieldRefs();
```

## 如何转化为Row

`HiveGeneralRowBuilder`扩展了如下接口，用以解析并构建row。

```
void build(Object objectValue, Row reuse, String mandatoryEncoding, RowTypeInfo rowTypeInfo) throws BitSailException {
```

在此方法中: 
 1. 按照[如何解析Writable](#jump_parse)获取解析需要使用的`deseiralizer`和`structFields`。
 2. 按照`rowTypeInfo`，依次从`columnMapping`中获取字段在hive中的`structField`，并以此解析出原始数据。
 3. 根据`rowTypeInfo`中的数据类型将解析后的原始数据转化为相应的`Column`，再存储到`reuse`中。

以上面提到的 [`test_db.test_table`](#jump_example_table) 为例，可按照如下方式构建`rowTypeInfo`和`columnMapping`: 

```
import com.bytedance.bitsail.flink.core.typeinfo.PrimitiveColumnTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;

TypeInformation<?>[] fieldTypes = new TypeInformation[] {
  PrimitiveColumnTypeInfo.LONG_COLUMN_TYPE_INFO,
  PrimitiveColumnTypeInfo.STRING_COLUMN_TYPE_INFO,
  PrimitiveColumnTypeInfo.STRING_COLUMN_TYPE_INFO
};
RowTypeInfo rowTypeInfo = new RowTypeInfo(
  fieldTypes,
  new String[] {"id_field", "state_field", "county_field"}
);

Map<String, Integer> columnMapping = ImmutableMap.of(
  "id_field", 0,
  "state_field", 1,
  "county_field", 2
);
```
用上面的`columnMapping`构建`HiveGeneralRowBuilder`和`rowTypeInfo`调用build方法后，即可获得内容按字段顺序为`id`, `state`, `county`的`row`。 

## 支持的数据类型

`HiveGeneralRowBuiler`支持解析常见的Hive内置数据类型，包括所有的基础数据类型，以及Map和List两种复杂数据类型。

基础数据类型解析后以 `com.bytedance.bitsail.common.column.Column` 类型存储，并支持一定的数据类型转化，具体见下表:

| hive数据类型                             | 支持的转化类型                                                                                            | 说明                                                                                                                                                                                                      |
|--------------------------------------|----------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| TINYINT<br>SMALLINT<br>INT<br>BIGINT | 1. `StringColumn`<br>2. `LongColumn`<br>3. `DoubleColumn`<br>                                      | 以`1234L`为例，转化后的数据分别为:<br> 1. `StringColumn`: `"1234"`<br>2. `LongColumn`: `1234`<br>3. `DoubleColumn`: `1234.0`                                                                                         |
| BOOLEAN                              | 1. `StringColumn`<br>2. `BooleanColumn`                                                            | 以`false`为例，转化后的数据分别为:<br> 1. `StringColumn`: `"false"`<br> 2. `BooleanColumn`: `false`                                                                                                                  |
| FLOAT<br>DOUBLE<br>DECIMAL           | 1. `StringColumn`<br>2. `DoubleColumn`                                                             | 以`3.141592`为例，转化后的数据分别为:<br> 1. `StringColumn`: `"3.141592"`<br> 2. `DoubleColumn`: `3.141592`                                                                                                          |
| STRING<br>CHAR<br>VARCHAR            | 1. `StringColumn`<br>2. `LongColumn`<br>3. `DoubleColumn`<br>4. `BooleanColumn`<br>5. `DateColumn` | 1. `LongColumn`: 使用`BigDecimal`来处理字符类型数据，字符串需要满足`BigDecimal`的格式需求。<br>2. `DoubleColumn`: 使用`Double.parseDouble`处理字符类型浮点数，字符串需要满足`Double`格式需求。<br>3. `BooleanColumn`: 仅识别字符串`"0", "1", "true", "false"`。 |
| BINARY                               | 1. `StringColumn`<br>2. `BytesColumn`                                                              | 以`byte[]{1, 2, 3}`为例，转化后的数据分别为:<br> 1. `StringColumn`: `"[B@1d29cf23"`<br> 2. `BytesColumn`: `AQID`                                                                                                     |
| TIMESTAMP                            | 1. `StringColumn`<br>2. `LongColumn`                                                               | 以 `2022-01-01 10:00:00`为例，转化后的数据分别为:<br>1. `StringColumn`: `"2022-01-01 10:00:00"`<br>2. `LongColumn`: `1641002400`                                                                                     |
| DATE                                 | 1. `StringColumn`<br> 2. `DateColumn` 3. `LongColumn`                                              | 以 `2022-01-01`为例，转化后的数据分别为:<br>1. `StringColumn`: `"2022-01-01"`<br>2. `DateColumn`: `2022-01-01`<br>3. `LongColumn`: `1640966400`                                                                      |

## 使用示例

使用上面提到的[`test_db.test_table`](#jump_example_table)为例，下面的代码展示了如何转化这张hive表中读取的`Writable`数据为想要的`Row`数据。


| 字段名      | 字段类型     |
|----------|----------|
| `id`     | `BIGINT` |
| `state`  | `STRING` |
| `county` | `STRING` |

- 元数据存储的thrift URI为 `thrift://localhost:9083`

```
/**
 * @param rawData Writable data for building a row.
 */
public Row buildRow(Writable rawData) {
  // 1. Initialize hive row builder
  String database = "test_db";
  String table = "test_table";
  Map<String, Integer> columnMapping = ImmutableMap.of(
    "id", 0,
    "state", 1,
    "county", 2
  );
  Map<String, String> hiveProperties = ImmutableMap.of(
    "metastore_uri", "thrift://localhost:9083"
  );
  RowBuilder rowBuilde = new HiveGeneralRowBuilder(
    columnMapping, database, table, hiveProperties
  );

  // 2. Construct row type infomation.
  TypeInformation<?>[] typeInformationList = {
    PrimitiveColumnTypeInfo.LONG_COLUMN_TYPE_INFO,
    PrimitiveColumnTypeInfo.STRING_COLUMN_TYPE_INFO,
    PrimitiveColumnTypeInfo.STRING_COLUMN_TYPE_INFO
  };
  RowTypeInfo rowTypeInfo = new RowTypeInfo(typeInformationList,
    new String[] {"id", "state", "county"}
  );
  
  // 3. Parse rawData and build row.
  Row reuse = new Row(3);
  rowBuilder.build(rawData, reuse, "UTF-8", rowTypeInfo);
  return reuse;
}
```