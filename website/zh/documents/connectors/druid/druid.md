# Druid 连接器

上级文档：[连接器](../README.md)

**BitSail** Druid连接器支持批式读写Druid资料源。

## 依赖引入

```xml
<dependency>
   <groupId>com.bytedance.bitsail</groupId>
   <artifactId>connector-druid</artifactId>
   <version>${revision}</version>
</dependency>
```

-----

## Druid写入

### 支持的数据类型

支持写入常见的Druid数据类型:

- 整型 Long
- 浮点类型 Float, Double
- 字符类型 String

### 支持的操作类型

支持以下操作类型:
 - INSERT


### 主要参数

写连接器参数在`job.writer`中配置，实际使用时请注意路径前缀。示例:

```json
{
  "job": {
    "writer": {
      "class": "com.bytedance.bitsail.connector.druid.sink.DruidSink",
      "datasource": "testDruidDataSource",
      "coordinator_url": "localhost:8888"
    }
  }
}
```


#### 必需参数

| 参数名称                      | 是否必填 | 参数枚举值 | 参数含义                                                                |
|:--------------------------|:-----|:------|:--------------------------------------------------------------------|
| class                     | 是  |       | Druid写连接器类型, `com.bytedance.bitsail.connector.druid.sink.DruidSink` |
| datasource          | 是 | | 要写入的Druid资料源                                                        |
| coordinator_url | 是 | | Druid master地址, 格式是 `<host>:<port>`                                 |
| columns                   | 是 | | 要写入的数据列的列名和类型                                                       |

-----

## 相关文档

配置示例文档：[Druid 连接器示例](./druid-example.md)
