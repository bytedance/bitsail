# bitsail-component-formats-flink

-----

Parent document: [bitsail-components](../README.md)

## Content

When **BitSail** uses flink as the engine, it uses `flink rows` as intermediate format.
So developers need to convert data from data source into `flink rows`.
This module offers convenient methods to convert some kinds of data into `flink rows`.
The specific supported formats are as follows:

so it needs to be converted to after reading the data from the data source bitsail rows. This module is used to provide bitsail rowsa . The specific supported formats are as follows:

 - The `flink rows` mentioned above are actually `com.bytedance.bitsail.common.column.Column` data wrapped by `org.apache.flink.types.Row`.

| Name                                  | Function                                        | Link                     |
|---------------------------------------|-------------------------------------------------|--------------------------|
| `bitsail-component-format-flink-api`  | Provide interface for converting data to `Row`  | [link](#jump_api)        |
| `bitsail-component-format-flink-hive` | Providing method for converting hive `Writable` | [link](hive-format.md)   |
| `bitsail-component-format-flink-json` | Providing method for converting json string     | [link](./json-format.md) |

-----

### <span id="jump_api">RowBuilder interface</span>

As the name implies, `RowBuilder` is a builder that converts raw data in a certain format into `Row`.
Therefore it requires at least two parameters:
 
 1. `value`: Raw data, such as Writables, JSON strings, <i>etc.</i>.
 2. `rowTypeInfo`: Describing the structure of raw data, so we can know that what the fields name and data type to extract.

A basic `build` method is as follows:


```
/**
 * @param value Raw data to transform to 'bitsail rows'.
 * @param reuse The transformed `bitsail row`.
 * @param rowTypeInfo Determine the format (field name, data types) of the transformed row.
 */
void build(Object value, Row reuse, RowTypeInfo rowTypeInfo) throws BitSailException;
```

In addition to the above parameters, the `build` process can also be guided by other parameters, such as character encoding, the index range of fields <i>etc.</i>. So `RowBuilder` provides the following two extension interfaces:


```
void build(Object value, Row reuse, String mandatoryEncoding, RowTypeInfo rowTypeInfo) throws BitSailException;

/**
 * @param fieldIndexes Indices of fields in row data that should be included while building.
 */
void build(Object value, Row reuse, String mandatoryEncoding, RowTypeInfo rowTypeInfo, int[] fieldIndexes);
```


