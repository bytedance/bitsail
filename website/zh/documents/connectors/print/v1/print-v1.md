# Print-V1 连接器

上级文档: [connectors](../../README.md)

***BitSail*** Print写连接器将上游过来的数据打印出来，目前在Flink Task Manager的Stdout中可以看到。

## 依赖引入

```xml
<dependency>
    <groupId>com.bytedance.bitsail</groupId>
    <artifactId>connector-print</artifactId>
    <version>${revision}</version>
</dependency>
```

## Print输出

### 支持数据类型

Print连接器对数据类型没有限制

### 主要参数

写连接器参数在`job.writer`中配置，实际使用时请注意路径前缀。示例:

```json
{
  "job": {
    "writer": {
      "class": "com.bytedance.bitsail.connector.print.sink.PrintSink",
      "sample_write": true,
      "sample_limit": 10
    }
  }
}
```

#### 必需参数

| 参数名称  | 是否必填 | 参数枚举值 | 参数含义                                                                |
|:------|:-----|:------|:--------------------------------------------------------------------|
| class | 是    |       | Print写连接器类型, `com.bytedance.bitsail.connector.print.sink.PrintSink` |

#### 可选参数

| 参数名称         | 是否必填 | 参数枚举值 | 参数含义     | 默认值   |
|:-------------|:-----|:------|:---------|:------|
| sample_write | 否    |       | 是否采样写入日志 | false |
| sample_limit | 否    |       | 采样频率     | 5     |

## 相关文档

配置示例文档: [print-connector-example](./print-v1-example.md)
