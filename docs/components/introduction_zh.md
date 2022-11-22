# bitsail-components

-----

### 内容

本模块包含多种可用于应用开发的基本功能组件。目前我们支持了五种基本的功能模块：

- `bitsail-component-clients`:
  - 支持创建各种client，例如`KafkaProducer`。
  - 细节可参考：[bitsail_component_clients](./clients/introduction_zh.md)


- `bitsail-component-formats-flink`:
    - 支持将各种数据源的数据类型（例如hive的 `Writables`）转化为 `bitsail rows`。
    - 细节可参考：[bitsail_component_formats_flink](./format/introduction_zh.md)


- `bitsail-conversion-flink`:
    - 支持将 `bitsail rows` 转化为各种数据源的数据类型（比如hive的 `Writables`）。
    - 细节可参考：[bitsail_conversion_flink](./conversion/introduction_zh.md)


- `bitsail-flink-row-parser`:
    - 支持按照指定格式解析 `bytes` 数组为 `bitsail rows`。
    - 细节可参考：[bitsail_flink_row_parser](./parser/introduction_zh.md)
    
-----

### 如何使用

开发者可通过如下方式导入依赖来使用相应的功能模块：

```
<dependency>
    <groupId>com.bytedance.bitsail</groupId>
    <artifactId>bitsail-xxxxx</artifactId>
    <version>${revision}</version>
    <scope>compile</scope>
</dependency>
```

