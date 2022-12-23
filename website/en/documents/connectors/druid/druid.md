# Druid connector

Parent document: [Connectors](../README.md)

**BitSail** Druid connector supports writing druid data-sources.

## Maven dependency

```xml
<dependency>
   <groupId>com.bytedance.bitsail</groupId>
   <artifactId>connector-druid</artifactId>
   <version>${revision}</version>
</dependency>
```

-----

## Druid Writer

### Supported data type

Support common Druid data types:

- Long
- Float
- Double
- String

### Supported operation type

Support the following operations:

- INSERT


### Parameters

The following mentioned parameters should be added to `job.writer` block when using, for example:

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


#### Necessary parameters

| Param name             | Required | Optional value | Description                                                                       |
|:-----------------------|:---------|:---------------|:----------------------------------------------------------------------------------|
| class                  | yes  |       | Druid writer's class name, `com.bytedance.bitsail.connector.druid.sink.DruidSink` |
| datasource             | yes | | Druid DataSource to write                                                         |
| coordinator_url        | yes | | Druid master addresses. Format is `<host>:<port>`                                 |
| columns                | yes | | The name and type of columns to write                                             |

-----

## Related documents

Configuration example: [Druid connector example](./druid-example.md)
