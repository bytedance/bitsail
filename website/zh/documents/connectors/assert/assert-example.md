# Assert 连接器示例

上级文档：[Assert 连接器](./assert.md)

下面配置的具体含义：
- 数据的总条数范围为 `[10, 20]`
- `name` 列非空，字符串长度范围为 `[1, 1000]`
- `price` 列非空，取值范围是 `[2, 180]`

```json
{
  "job": {
    "writer": {
      "class": "com.bytedance.bitsail.connector.assertion.sink.AssertSink",
      "columns": [
        {
          "name": "name",
          "type": "string"
        },
        {
          "name": "price",
          "type": "double"
        }
      ],
      "row_rules": {
        "min_row": 10,
        "max_row": 20
      },
      "column_rules": {
        "name": {
          "not_null": true,
          "min_len": 1,
          "max_len": 1000
        },
        "price": {
          "not_null": true,
          "min": 2,
          "max": 180
        }
      }
    }
  }
}
```
