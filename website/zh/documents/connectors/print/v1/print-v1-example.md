# Print-V1 连接器配置示例

父目录: [print-connector](./print-v1.md)

-----

## Print写连接器

不管上游来的数据有多少个字段、是什么数据类型，都可以用如下配置将数据打印出来。

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