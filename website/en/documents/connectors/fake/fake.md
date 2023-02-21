# Fake连接器

Parent document: [connectors](../README.md)

***BitSail*** Fake is a read connector. After you specify the name and type of some columns, the Fake connector will generate the specified number of data rows for you; it is a good helper for functional testing, and is usually the read connector for the HelloWorld program for BitSail beginners.


## Maven depedency

```xml
<dependency>
   <groupId>com.bytedance.bitsail</groupId>
   <artifactId>bitsail-connector-fake</artifactId>
   <version>${revision}</version>
</dependency>
```

## Fake Reader

### Supported data types

- Basic Data types:
    - Integer type:
        - short
        - int
        - long
        - biginteger
    - Float type:
        - float
        - double
        - bigdecimal
    - Time type:
        - time
        - timestamp
        - date
        - date.date
        - date.time
        - date.datetime
    - String type:
        - string
    - Bool type:
        - not support
    - Binary type:
        - binary
        - bytes
    

### Parameters

The following mentioned parameters should be added to job.reader block when using, for example:

```json
{
  "job": {
    "reader": {
      "class": "com.bytedance.bitsail.connector.legacy.fake.source.FakeSource",
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



#### Necessary parameters

| Param name                   | Required | Optional value | Description                                                                                                    |
|:------------------|:-----|:------|:------------------------------------------------------------------------------------------|
| class             | yes  |       | Fake reader's class name, `com.bytedance.bitsail.connector.legacy.fake.source.FakeSource` |
| total_count       | yes | | total number of data to generate  |

#### Optional parameters






| Param name                   | Required | Optional value | Description                                                                                                    |
|:----------------------------------------|:------|:------|:-----------------------------------------------------|
| rate | no |       | The frequency of data generation, the larger the value, the more data generated per unit time                  |
| lower_limit |  no | | Together with upper_limit, it is used as the seed to generate float, double, bigdecimal type fields, not the boundary of field values |
| upper_limit | no | | Together with lower_limit, it is used as the seed to generate float, double, bigdecimal type fields, not the boundary of field values |
| from_timestamp |  no | | Together with to_timestamp, it is used as the seed for generating time type fields, not the boundary of field values |
| to_timestamp | no | | Together with from_timestamp, it is used as the seed for generating time type fields, not the boundary of field values |


## Related document

Configuration example:  [fake-connector-example](./fake-example.md)
