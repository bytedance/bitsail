# bitsail-conversion-flink

-----

上级文档: [bitsail-components](../README.md)

## 内容

**BitSail** 在将数据传输到指定数据源时，需要将传输过程中使用的中间格式 `bitsail rows` 转化为数据源可接受的数据类型。
本模块提供了用于转化`bitsail rows`为其他数据类型的便捷工具。

- 文中`bitsail rows`指由`org.apache.flink.types.Row`包住的`com.bytedance.bitsail.common.column.Column`数据。

具体支持的数据类型如下：

| 子模块                             | 支持的功能                        | 链接                         |
|---------------------------------|------------------------------|----------------------------|
| `bitsail-conversion-flink-hive` | 提供转化`Row`为hive `Writable`的方法 | [link](hive-convert.md) |


