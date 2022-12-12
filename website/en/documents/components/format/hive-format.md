# bitsail-component-format-flink-hive

-----

Parent document: [bitsail-component-format-flink](./introduction.md)


This module provides `HiveGeneralRowBuilder` for supportinig converting hive `Writable` data into `Row`.


## how to use

The working principle is to first obtain the meta information of the target hive table from the hive metastore, and then convert data according to the meta information `ObjectInspector`. 

So we need two kinds of parameters to construct a `HiveGeneralRowBuilder`:

1. Parameters for getting hive meta information:
    - `database`: hive database name
    - `table`: hive table name
    - `hiveProperties`: Properties of hive configuration to connect to hive metastore, which is stored as a Map.
2. `columnMapping`: The fields order of row to construct, which  si stored as Map<String, Integer>. Map key is the field name, while value is the index of this field in hive table.


### Example

Take the following hive table <span id="jump_example_table">`test_db.test_table`</span> as a example: 
- Thrift uri for hive metastore is: `thrift://localhost:9083`

| field name | field type |
|------------|------------|
| `id`       | `BIGINT`   |
| `state`    | `STRING`   |
| `county`   | `STRING`   |

So we can use the following codes to construct a `HiveGeneralRowBuilder`:

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

## <span id="jump_parse">How to parse writable</span>

To parse `Writable` data, one needs `deserializer` and `ObjectInspector` information from hive table.

`HiveGeneralRowBuilder` supports getting these meta information according to the hive information (including database, table, and some other properties used to connect to the metastore) passed in by the user.


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

## How to Convert to Row

`HiveGeneralRowBuilder` implements the following interface to convert hive data to a `Row`:

```
void build(Object objectValue, Row reuse, String mandatoryEncoding, RowTypeInfo rowTypeInfo) throws BitSailException {
```

In this method, it has three steps:

1. According to ["How to parse writable"](#jump_parse), it gets the `deseiralizer` and `structFields` for parsing.

2. According to `rowTypeInfo`, it extracts fields in order from `columnMapping` and `structField`. 
Based on these two information, it can extract the raw data.
   
3. According to the field type in `rowTypeInfo`, it converts the extracted raw data into `com.bytedance.bitsail.common.column.Column`, and then wraps it with `org.apache.flink.types.Row`.


Take the above mentioned hive table [`test_db.test_table`](#jump_example_table) as an example, one can build `rowTypeInfo` and `columnMapping` as follows:


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

Using the above `rowTypeInfo` and `columnMapping`, one can get a `row` of field `id`, `state`, `county` by calling build method.


## Supported data types

`HiveGeneralRowBuiler` supports parsing common hive built-in data types, including all basic data types, and two complex data types, Map and List.

We support some types of data type conversion as follows: 


| Hive data type                       | You can convert the hive data type to                                                              | Description                                                                                                                                                                                                       |
|--------------------------------------|----------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| TINYINT<br>SMALLINT<br>INT<br>BIGINT | 1. `StringColumn`<br>2. `LongColumn`<br>3. `DoubleColumn`<br>                                      | Take `1234L` as an example，the converted columns are:<br> 1. `StringColumn`: `"1234"`<br>2. `LongColumn`: `1234`<br>3. `DoubleColumn`: `1234.0`                                                                   |
| BOOLEAN                              | 1. `StringColumn`<br>2. `BooleanColumn`                                                            | Take `false` as an example，the converted columns are:<br> 1. `StringColumn`: `"false"`<br> 2. `BooleanColumn`: `false`                                                                                            |
| FLOAT<br>DOUBLE<br>DECIMAL           | 1. `StringColumn`<br>2. `DoubleColumn`                                                             | Take `3.141592` as an example，the converted columns are:<br> 1. `StringColumn`: `"3.141592"`<br> 2. `DoubleColumn`: `3.141592`                                                                                    |
| STRING<br>CHAR<br>VARCHAR            | 1. `StringColumn`<br>2. `LongColumn`<br>3. `DoubleColumn`<br>4. `BooleanColumn`<br>5. `DateColumn` | 1. `LongColumn`: Use `BigDecimal` to convert string to integer.<br>2. `DoubleColumn`: Use `Double.parseDouble` to convert string to float number<br>3. `BooleanColumn`: Only recognize`"0", "1", "true", "false"` |
| BINARY                               | 1. `StringColumn`<br>2. `BytesColumn`                                                              | Take `byte[]{1, 2, 3}` as an example，the converted columns are:<br> 1. `StringColumn`: `"[B@1d29cf23"`<br> 2. `BytesColumn`: `AQID`                                                                               |
| TIMESTAMP                            | 1. `StringColumn`<br>2. `LongColumn`                                                               | Take `2022-01-01 10:00:00` as an example，the converted columns are:<br>1. `StringColumn`: `"2022-01-01 10:00:00"`<br>2. `LongColumn`: `1641002400`                                                                |
| DATE                                 | 1. `StringColumn`<br> 2. `DateColumn` 3. `LongColumn`                                              | Take `2022-01-01` as example，the converted columns are:<br>1. `StringColumn`: `"2022-01-01"`<br>2. `DateColumn`: `2022-01-01`<br>3. `LongColumn`: `1640966400`                                                    |

## Example

Take the above mentioned hive table [`test_db.test_table`](#jump_example_table) as an example，the following codes show how to convert the `Writable` data in thie hive table to `Row`.



| field name | field type |
|------------|------------|
| `id`       | `BIGINT`   |
| `state`    | `STRING`   |
| `county`   | `STRING`   |

- Thrift uri of metastore: `thrift://localhost:9083`

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