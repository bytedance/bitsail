# Fake连接器

上级文档: [fake-example_zh.md](fake-example_zh.md)

如下展示了如何通过fake数据源生成随机数

```json

{
  "job": {
    "reader": {
      "class": "com.bytedance.bitsail.connector.fake.source.FakeSource",
      "total_count": 300,
      "rate": 10,
      "columns": [
        {
          "name": "id",
          "type": "long",
          "properties": "unique"
        },
        {
          "name": "id",
          "type": "date"
        },
        {
          "name": "list_value",
          "type": "list<string>"
        },
        {
          "name": "map_value",
          "type": "map<string,string>"
        },
        {
          "name": "local_datetime_value",
          "type": "timestamp"
        },
        {
          "name": "date_value",
          "type": "date.date"
        },
        {
          "name": "datetime_value",
          "type": "date.datetime"
        }
      ]
    }
  }
}
```