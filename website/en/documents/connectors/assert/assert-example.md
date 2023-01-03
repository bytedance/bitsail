# Assert connector example

Parent document: [Assert connector](./assert.md)

The exact meaning of the following configuration.
- Total number of data entries in the range `[10, 20]`
- The `name` column is not-null and has a string length in the range `[1, 1000]`
- The `price` column is not-null and has a value in the range `[2, 180]`

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
