# Assert connector

Parent document: [Connectors](../README.md)

**BitSail** Assert connector can validate data against user-defined rules. The main function points are as follows:

- Support multiple custom check rules

## Maven dependency

```xml
<dependency>
    <groupId>com.bytedance.bitsail</groupId>
    <artifactId>connector-assert</artifactId>
    <version>${revision}</version>
    <scope>provided</scope>
</dependency>
```

## Supported data types

- Basic data types supported:
    - Integer type:
        - tinyint
        - smallint
        - int
        - bigint
    - Float type:
        - float
        - double
        - decimal
    - Time type:
        - timestamp
        - date
    - String type:
        - string
        - varchar
        - char
    - Bool type:
        - boolean

### Parameters

The following mentioned parameters should be added to `job.writer` block when using, for example:


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

#### Necessary parameters

| Param name | Required | Optional value | Description                                                                             |
|:-----------|:---------|:---------------|:----------------------------------------------------------------------------------------|
| class      | yes      |                | Assert writer's class name, `com.bytedance.bitsail.connector.assertion.sink.AssertSink` |
| columns    | yes      |                | The name and type of columns to write                                                   |

#### Optional parameters

| Param name             | Required | Optional value | Description              |
|:-----------------------|:---------|:---------------|:-------------------------|
| writer_parallelism_num | no       |                | Writer parallelism num   |
| row_rules              | no       |                | Custom row check rule    |
| column_rules           | no       |                | Custom column check rule |


#### Check rules

| Rule     | Description                                | Parameter Type |
|:---------|:-------------------------------------------|:---------------|
| min_row  | The minimum number of rows                 | int            |
| max_row  | The maximum number of rows                 | int            |
| not_null | The value can't be null                    | boolean        |
| min      | The minimum value of data                  | double         |
| max      | The maximum value of data                  | double         |
| min_len  | The minimum string length of a string data | int            |
| max_len  | The maximum string length of a string data | int            |


#### Descriptions

- If `row_rules` is declared, the parallelism of `Assert Sink` will be forced to `1` and the custom `writer_parallelism_num` parameter value will be disabled.

## Related documents

Configuration examples: [Assert connector example](./assert-example.md)
