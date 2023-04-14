# Print connector

Parent document: [connectors](../README.md)

***BitSail*** The Print write connector prints the data from the upstream, which can be seen in the Stdout of the Flink
Task Manager.

## Maven dependency

```xml
<dependency>
    <groupId>com.bytedance.bitsail</groupId>
    <artifactId>bitsail-connector-print</artifactId>
    <version>${revision}</version>
</dependency>
```

## Print Writer

### Supported data type

The Print connector has no restrictions on data types

### Parameters

The following mentioned parameters should be added to `job.writer` block when using, for example:

```json
{
  "job": {
    "writer": {
      "class": "com.bytedance.bitsail.connector.legacy.print.sink.PrintSink",
      "batch_size": "10"
    }
  }
}
```

#### Necessary parameters

| Param name | Required | Optional value | Description                                                                            |
|:-----------|:---------|:---------------|:---------------------------------------------------------------------------------------|
| class      | yes      |                | Print writer class name, `com.bytedance.bitsail.connector.legacy.print.sink.PrintSink` |

#### Optional parameters

| Param name | Required | Optional value | Description                           |
|:-----------|:---------|:---------------|:--------------------------------------|
| batch_size | no       |                | Specify the batch size for each write |

## Related document

Configuration examples: [print-connector-example](./print-example.md)
