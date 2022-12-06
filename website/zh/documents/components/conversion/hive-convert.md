# bitsail-convertion-flink-hive

-----

本模块中的`HivewritableExtractor`支持将bitsail支持的`Row`转化为hive `Writable`数据。
转化完成后，用户可使用`org.apache.hadoop.hive.ql.exec.RecordWriter`将转化后的`Writable`数据方便地写入hive中。

下文将详细介绍`GeneralWritableExtractor`，一种`HivewritableExtractor`的通用实现，可用于多种存储格式的hive表，例如parquet、orc、text等。

## 支持的数据类型

`GeneralWritableExtractor`支持常见的hive基础数据类型，以及List、Map、Struct复杂数据类型。
常见的基础数据类型包括：
```text
 TINYINT
 SMALLINT
 INT
 BIGINT
 BOOLEAN
 FLOAT
 DOUBLE
 DECIMAL
 STRING
 BINARY
 DATE
 TIMESTAMP
 CHAR
 VARCHAR
```

## 转化功能

除了按照类型进行基础的转化外，`GeneralWritableExtractor`还支持通过选项支持其他的一些转化功能。
这些选项统一在 `com.bytedance.bitsail.conversion.hive.ConvertToHiveObjectOptions` 管理，主要包括以下几种:

```java
public class ConvertToHiveObjectOptions implements Serializable {

  private boolean convertErrorColumnAsNull;
  private boolean dateTypeToStringAsLong;
  private boolean nullStringAsNull;
  private DatePrecision datePrecision;

  public enum DatePrecision {
    SECOND, MILLISECOND
  }
}
```

### convertErrorColumnAsNull

当数据转化出现错误时，此选项可决定报出错误或者忽略错误并转化为null。

例如，在转化字符串 `"123k.i123"` 为Double类型的hive数据时，由于字符串不能被识别为一个浮点数，会产生报错。
 - 若`convertErrorColumnAsNull=true`，则忽略此报错，并将此字符串转化为`null`。
 - 若`convertErrorColumnAsNull=false`，则报出转化错误。


### dateTypeToStringAsLong

若传入的`Row`中需要转化的字段类型为`com.bytedance.bitsail.common.column.DateColumn`，且该column使用`java.sql.Timestamp` 初始化时，则该字段会被转化为时间戳数据。


### nullStringAsNull

若传入的`Row`中需要转化的字段数据为null，且目标hive数据类型为字符串时，用户可选择转化成 null 或者 空字符串 ""。

| 参数                 | 值       | 转化               |
|--------------------|---------|------------------|
| `nullStringAsNull` | `true`  | `null` -> `null` |
| `nullStringAsNull` | `false` | `null` -> `""`   |


### datePrecision

此选项决定`Date`数据类型转化为时间戳时的精度，可选值为`SECOND`和`MILLISECOND`。

例如，在转化日期"2022-01-01 12:34:56"为时间戳时，根据`datePrecision`，会返回不同的值：
 - `datePrecision=SECOND`: 1641011696
 - `datePrecision=MILLISECOND`: 1641011696000


## 如何使用

下面介绍如何使用`GeneralHiveExtractor`。

### 初始化

在创建`GeneralHiveExtractor`实例后，还需要以下几步进行初始化:

 #### 1. 设置<b>字段映射</b>和<b>字段名</b>

fieldNames和columnMapping共同决定了`Row`中的字段写入hive的顺序。

   - columnMapping: 以Map形式存储的字段名到hive中字段位置的映射。
   - fieldNames: 需要转化的`Row`中的字段名。

下面举例说明这两个参数如何设置的:

 - 要写入的hivec测试表<span id="hive_example">hive_example</span>结构如下:

| 字段名       | 字段类型     | index |
|-----------|----------|-------|
| `field_c` | `STRING` | 0     |
| `field_b` | `STRING` | 1     |
| `field_a` | `STRING` | 2     |

 - 要转化的`Row`内容如下:
```json
[
  {"name": "row_field_0", "type": "string", "data": "0"},
  {"name": "row_field_1", "type": "string", "data": "1"},
  {"name": "row_field_2", "type": "string", "data": "2"}
]
```
 
 - 根据`Row`内容，`fieldNames`的内容需要设置为:
```java   
String fieldNames = {"field_0", "field_1", "field_2"};
```
 - 若想完成 `row_field_0->field_a`,`row_field_1->field_b`, `row_field_2->field_c`的映射，则将columnMapping设置为如下值:
```
Map<String, Integer> columnMapping = ImmutableMap.of(
  "row_field_0", 2,
  "row_field_1", 1,
  "row_field_2", 0
);
```

#### 2. 设置转化参数

用户自行构建用于转化的`ConvertToHiveObjectOptions`后，可设置到`GeneralWritableExtractor`。

```
ConvertToHiveObjectOptions options = ConvertToHiveObjectOptions.builder()
  .convertErrorColumnAsNull(false)
  .dateTypeToStringAsLong(false)
  .datePrecision(ConvertToHiveObjectOptions.DatePrecision.SECOND)
  .nullStringAsNull(false)
  .build();

  hiveWritableExtractor.initConvertOptions(options);
```

#### 3. 初始化ObjectInspector

`GeneralWritableExtractor`提供了接口初始化用于转化的ObjectInspector。

```
public SettableStructObjectInspector createObjectInspector(final String columnNames, final String columnTypes);
```
其中: 
 - `columnNames`: hive中字段名以 `,` 分隔组成的字符串， 顺序和hive中一致。
 - `columnsType`: hive中字段类型以 `,` 分隔组成的字符串， 顺序和hive中一致。

以上面的 [hive_example](#hive_example)为例:
```
String columnNames = "field_c,field_b,field_a";
String columnTypes = "string,string,string";
```

### 示例代码

```
/**
   * Hive table schema is:
   *     | field_name | field_type | field_index |
   *     | field_0    | bigint     | 0           |
   *     | field_1    | string     | 1           |
   *     | field_2    | double     | 2           |
   * Hive serde class is: org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe
   *
   * Row structure is:
   *    {
   *      ("name":"field_a", "type":"long", "data":100),
   *      ("name":"field_b", "type":"string", "data":"str"),
   *      ("name":"field_c", "type":"double", "data":3.14),
   *    }
   * 
   * @param serDe Initialized serializer. See `org.apache.hadoop.hive.serde2.Serializer`.
   * @param hiveWriter Initialized record writer. See `org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter`.
   */
  public void transformAndWrite(Serializer serDe, FileSinkOperator.RecordWriter hiveWriter) throws Exception {
    // 0. Initialize parameters.
    String[] fieldNames = {"field_a", "field_b", "field_c"};
    Map<String, Integer> columnMapping = ImmutableMap.of(
      "field_a", 0,
      "field_b", 1,
      "field_c", 2
    );
    ConvertToHiveObjectOptions options = ConvertToHiveObjectOptions.builder()
      .convertErrorColumnAsNull(false)
      .dateTypeToStringAsLong(false)
      .datePrecision(ConvertToHiveObjectOptions.DatePrecision.SECOND)
      .nullStringAsNull(false)
      .build();
    String hiveColumnNames = "field_0,field_1,field_2";
    String hiveColumnTypes = "bigint,string,double";

    // 1. Prepare a row.
    Row row = new Row(3);
    row.setField(0, new LongColumn(100));
    row.setField(1, new StringColumn("str"));
    row.setField(2, new DoubleColumn(3.14));

    // 2. Create GeneralWritableExtractor instance.
    GeneralWritableExtractor extractor = new GeneralWritableExtractor();
    extractor.setColumnMapping(columnMapping);
    extractor.setFieldNames(fieldNames);
    extractor.initConvertOptions(options);
    ObjectInspector inspector = extractor.createObjectInspector(hiveColumnNames, hiveColumnTypes);

    // 3. Transform row and write it to hive.
    Object hiveRow = extractor.createRowObject(row);
    Writable writable = serDe.serialize(hiveRow, inspector);
    hiveWriter.write(writable);
  }
```
