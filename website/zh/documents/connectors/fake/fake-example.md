# Fake连接器配置示例

父目录: [fake-connector](./fake.md)

-----



## Fake读连接器 

假设你想要生成一个300条的数据集，并且指定每个record有2个字段分别为name、age，则可用如下配置读取。

```json
{
  "job": {
    "reader": {
      "class": "com.bytedance.bitsail.connector.fake.source.FakeSource",
      "total_count": 300,
      "rate": 100,
      "random_null_rate": 0.1,
      "columns": [
        {
          "name": "name",
          "type": "string"
        },
        {
          "name": "age",
          "type": "int"
        }
      ]
    }
  }
}
```


## 设定常量值
你可以为每个字段指定常量值，例如下面的示例。 你可以在 time/timestamp/date/date.date/date.time/date.datetime 字段中将 defaultValue 设置为“now”。 list/map等集合字段只会生成一个元素，也可以在defaultValue attr中指定，需要注意的是map的key和value要用分隔 **_:\_**
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
          "name": "list_value",
          "type": "list<string>",
          "defaultValue": "test_element"
        },
        {
          "name": "map_value",
          "type": "map<string,string>",
          "defaultValue": "test-key_:_test-value"
        },
        {
          "name": "long_value",
          "type": "long",
          "defaultValue": "9223372036854775807"
        },
        {
          "name": "int_value",
          "type": "int",
          "defaultValue": "2147483647"
        },
        {
          "name": "short_value",
          "type": "short",
          "defaultValue": "32767"
        },
        {
          "name": "string_value",
          "type": "string",
          "defaultValue": "this is test constant string"
        },
        {
          "name": "boolean_value",
          "type": "boolean",
          "defaultValue": "false"
        },
        {
          "name": "double_value",
          "type": "double",
          "defaultValue": "13.5"
        },
        {
          "name": "float_value",
          "type": "float",
          "defaultValue": "17.8"
        },
        {
          "name": "bigdecimal_value",
          "type": "bigdecimal",
          "defaultValue": "17.8"
        },
        {
          "name": "biginteger_value",
          "type": "biginteger",
          "defaultValue": "17"
        },
        {
          "name": "sql_date_value",
          "type": "date.date",
          "defaultValue": "2023-01-22"
        },
        {
          "name": "sql_time_value",
          "type": "date.time",
          "defaultValue": "23:13:22"
        },
        {
          "name": "sql_timestamp_value",
          "type": "date.datetime",
          "defaultValue": "1674897553000"
        },
        {
          "name": "localdatetime_value",
          "type": "timestamp",
          "defaultValue": "2023-01-06 23:14:22.111"
        },
        {
          "name": "localdate_value",
          "type": "date",
          "defaultValue": "2023-01-22"
        },
        {
          "name": "localdate_value_now",
          "type": "date",
          "defaultValue": "now"
        }
      ]
    }
  }
}
```