# bitsail-conversion-flink

-----

Parent document: [bitsail-components](../README.md)

## Content

When **BitSail** transmits data to a specified data source, it needs to convert the intermediate format (`bitsail rows`) used in the transmission process into a data type acceptable to the data source. 
This module provides convenient tools for converting.

- In this context, `bitsail rows` means `com.bytedance.bitsail.common.column.Column` data wrapped by `org.apache.flink.types.Row`。

Specific supported data types are as follows:


| Name                            | Function                                           | Link                    |
|---------------------------------|----------------------------------------------------|-------------------------|
| `bitsail-conversion-flink-hive` | Provides a way to convert `Row` to hive `Writable` | [link](hive-convert.md) |


