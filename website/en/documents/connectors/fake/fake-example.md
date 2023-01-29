# Fake  connector examples

Parent document: [fake-connector](./fake.md)

-----



## Fake reader example

Suppose you want to generate a data set of 300 records, and specify that each record has two fields named name and age, you can use the following configuration to read.

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

## Specify constant value
you can specify constant value for each field fellow the example bellow. you can set defaultValue as "now" in time/timestamp/date/date.date/date.time/date.datetime field. only one element would be generated for collection fields like list/map,and you can specify them in defaultValue attr too, It should be noted that the key and value of the map should be separated by **_:\_**   
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