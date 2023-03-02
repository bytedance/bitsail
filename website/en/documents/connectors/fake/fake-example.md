# Fake  connector examples

Parent document: [fake-connector](./fake.md)

-----

## Fake reader example

Suppose you want to generate a data set of 300 records, and specify that each record has two fields named name and age,
you can use the following configuration to read.

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
