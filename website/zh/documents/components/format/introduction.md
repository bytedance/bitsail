# bitsail-component-formats-flink

-----

上级文档: [bitsail-components](../README.md)

## 内容

**BitSail** 在使用flink作为引擎时，各个connector的数据传输过程中使用 `bitsail rows` 作为传输中间格式，因此在将数据从数据源读入后需要转化为 `bitsail rows` 。
本模块用于提供将常见格式数据转化为 `bitsail rows` 的便捷方法，具体支持的格式如下：
 - 文中`bitsail rows`指由`org.apache.flink.types.Row`包住的`com.bytedance.bitsail.common.column.Column`数据。

| 子模块                                   | 支持的功能                   | 链接                        |
|---------------------------------------|-------------------------|---------------------------|
| `bitsail-component-format-flink-api`  | 提供`Row`转化的接口            | [link](#jump_api)         |
| `bitsail-component-format-flink-hive` | 提供 hive `Writable`的转化方法 | [link](hive-format.md) |
| `bitsail-component-format-flink-json` | 提供json格式字符串的转化方法        | [link](./json-format.md)  |

-----

### <span id="jump_api">`RowBuilder` 接口</span>

`RowBuilder`顾名思义，即将原始数据转化按照某种格式转化成Row的构建器。因此`RowBuilder`的转化方法需要至少两个必须的参数：
 1. `value`: 原始数据，例如Writables, JSON字符串等
 2. `rowTypeInfo`: 转化row的具体格式，例如字段名称&字段类型 

转化产生的产物`Row`使用参数的方式加载，开发者需要在`build`方法中对此`Row`进行赋值。
因此一个基础的构建接口如下：

```
/**
 * @param value Raw data to transform to 'bitsail rows'.
 * @param reuse The transformed `bitsail row`.
 * @param rowTypeInfo Determine the format (field name, data types) of the transformed row.
 */
void build(Object value, Row reuse, RowTypeInfo rowTypeInfo) throws BitSailException;
```

除了上面参数外，还可以通过其他的参数指导`Row`的构建，例如字符编码、包含的字段范围等。因此`RowBuilder`还提供了以下两个扩展接口:

```
void build(Object value, Row reuse, String mandatoryEncoding, RowTypeInfo rowTypeInfo) throws BitSailException;

/**
 * @param fieldIndexes Indices of fields in row data that should be included while building.
 */
void build(Object value, Row reuse, String mandatoryEncoding, RowTypeInfo rowTypeInfo, int[] fieldIndexes);
```


