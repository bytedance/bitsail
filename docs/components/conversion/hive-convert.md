# bitsail-convertion-flink-hive

-----

Parent document: [bitsail-conversion-flink](./introduction.md)

The `HiveWritableExtractor` in this module supports converting `Row` into hive `Writable` data.
After the converting, users can easily use `org.apache.hadoop.hive.ql.exec.RecordWriter` to write the converted data
into hive.

The following sections will introduce `GeneralWritableExtractor`, an implementation of `HiveWritableExtractor`, which
can be used for hive tables of various storage formats, such as parquet, orc, text, <i>etc.</i>.

## Supported data types

`GeneralWritableExtractor` support common hive basic data types, as well as List, Map, Struct complex data types.

Common data types include:

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

## Conversion

In addition to basic conversions by type，`GeneralWritableExtractor` also supports some other conversion functions
according to options.
These options are managed in `com.bytedance.bitsail.conversion.hive.ConvertToHiveObjectOptions`, including:

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

When there is an error in the data conversion, this option can decide to report the error or ignore the error and
convert to null.

For example, when converting a string "123k.i123" to double-type hive data, an error will be reported because the string
cannot be recognized as a float number.

- If `convertErrorColumnAsNull=true`, ignore this error and convert this string to null.
- If `convertErrorColumnAsNull=false`, a converting error exception is reported.

### dateTypeToStringAsLong

If the incoming Row field type to be converted is `com.bytedance.bitsail.common.column.DateColumn` which is initialized
by `java.sql.Timestamp`, this date column will be converted to a timestamp value.

### nullStringAsNull

If the incoming Rowfield data to be converted is null and the target hive data type is string, the user can choose to
convert it to null or an empty string "".

| Parameter | Condition | Converting |
| ----- |----------| --- |
|`nullStringAsNull`| `true`| `null` -> `null`|
|`nullStringAsNull`| `false`| `null` -> `""`|

### datePrecision

This option determines the precision when converting a date column to timestamp. The optional values are "SECOND" and "
MILLISECOND".

For example, when converting "2022-01-01 12:34:56" to timestamp, different value will be returned according to
datePrecision:

- `datePrecision=SECOND`: 1641011696
- `datePrecision=MILLISECOND`: 1641011696000

## how to use

The following describes how to develop with `GeneralHiveExtractor`.

### Initialization

After creating a `GeneralHiveExtractor` instance，the following steps are required:

#### 1. Set <b>column mapping</b> and <b>field names</b>

fieldNames and columnMapping determine the `Row` order in which the Hive fields are written.

- columnMapping:  The mapping of field names stored in Map form to field positions in hive.
- fieldNames: Row field names in order.

The following example shows how these two parameters are set:

- The structure of hive table to be written <span id="hive_example">hive_example</span> is:

| field name | data type | index |
|------------|-----------|-------|
| `field_c`  | `STRING`  | 0     |
| `field_b`  | `STRING`  | 1     |
| `field_a`  | `STRING`  | 2     |

- `Row` to convert:

```json
[
  {
    "name": "row_field_0",
    "type": "string",
    "data": "0"
  },
  {
    "name": "row_field_1",
    "type": "string",
    "data": "1"
  },
  {
    "name": "row_field_2",
    "type": "string",
    "data": "2"
  }
]
```

- According to `Row`, the `fieldNames` needs to be set as:

```java   
String fieldNames = {"field_0", "field_1", "field_2"};
```

- If want to achive mapping of `row_field_0->field_a`,`row_field_1->field_b`, `row_field_2->field_c`, the `olumnMapping`
  needs to be set as:

```
Map<String, Integer> columnMapping=ImmutableMap.of(
  "row_field_0",2,
  "row_field_1",1,
  "row_field_2",0
  );
```

#### 2. Set conversion parameters

After building a `ConvertToHiveObjectOptions`，developers need to set it to `GeneralWritableExtractor`。

```
ConvertToHiveObjectOptions options=ConvertToHiveObjectOptions.builder()
  .convertErrorColumnAsNull(false)
  .dateTypeToStringAsLong(false)
  .datePrecision(ConvertToHiveObjectOptions.DatePrecision.SECOND)
  .nullStringAsNull(false)
  .build();

  hiveWritableExtractor.initConvertOptions(options);
```

#### 3. Initialize ObjectInspector

`GeneralWritableExtractor` offers a interface to initialize ObjectInspector for converting.

```
public SettableStructObjectInspector createObjectInspector(final String columnNames,final String columnTypes);
```

- `columnNames`: A string consisting of field names in hive, separated by `','`, in the same order in hive.
- `columnsType`: A string consisting of data types in hive, separated by `','`, in the same order in hive.

Take the above test table ([hive_example](#hive_example)) as an example:

```
String columnNames="field_c,field_b,field_a";
  String columnTypes="string,string,string";
```

### Example code

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
public void transformAndWrite(Serializer serDe,FileSinkOperator.RecordWriter hiveWriter)throws Exception{
  // 0. Initialize parameters.
  String[]fieldNames={"field_a","field_b","field_c"};
  Map<String, Integer> columnMapping=ImmutableMap.of(
  "field_a",0,
  "field_b",1,
  "field_c",2
  );
  ConvertToHiveObjectOptions options=ConvertToHiveObjectOptions.builder()
  .convertErrorColumnAsNull(false)
  .dateTypeToStringAsLong(false)
  .datePrecision(ConvertToHiveObjectOptions.DatePrecision.SECOND)
  .nullStringAsNull(false)
  .build();
  String hiveColumnNames="field_0,field_1,field_2";
  String hiveColumnTypes="bigint,string,double";

  // 1. Prepare a row.
  Row row=new Row(3);
  row.setField(0,new LongColumn(100));
  row.setField(1,new StringColumn("str"));
  row.setField(2,new DoubleColumn(3.14));

  // 2. Create GeneralWritableExtractor instance.
  GeneralWritableExtractor extractor=new GeneralWritableExtractor();
  extractor.setColumnMapping(columnMapping);
  extractor.setFieldNames(fieldNames);
  extractor.initConvertOptions(options);
  ObjectInspector inspector=extractor.createObjectInspector(hiveColumnNames,hiveColumnTypes);

  // 3. Transform row and write it to hive.
  Object hiveRow=extractor.createRowObject(row);
  Writable writable=serDe.serialize(hiveRow,inspector);
  hiveWriter.write(writable);
  }
```
