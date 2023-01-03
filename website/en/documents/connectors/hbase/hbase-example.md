# Hbase connector example

Parent document: [HBase connector](./hbase.md)

## HBase configuration

- Using docker to start an HBase cluster in local, then the default configuration used by it is:
```json
{
  "hbase.zookeeper.quorum":"127.0.0.1",
  "hbase.zookeeper.property.clientPort":"2181"
}
```

- Create a table `test_table` with three column families:

```shell
create 'test_table', 'cf1', 'cf2', 'cf3'
```

## Reader Example

```json
{
  "job": {
    "common": {
      "job_id": -250134,
      "job_name": "bitsail_hbase_to_print_test",
      "instance_id": -2050134,
      "user_name": "user"
    },
    "reader": {
      "class": "com.bytedance.bitsail.connector.hbase.source.HBaseInputFormat",
      "table": "test_table",
      "hbase_conf":{
        "hbase.zookeeper.quorum":"127.0.0.1",
        "hbase.zookeeper.property.clientPort":"2181"
      },
      "columns": [
        {
          "index": 0,
          "name": "cf1:str1",
          "type": "bigint"
        },
        {
          "index": 1,
          "name": "cf1:int1",
          "type": "string"
        },
        {
          "index": 2,
          "name": "cf2:str2",
          "type": "bigint"
        },
        {
          "index": 3,
          "name": "cf3:int3",
          "type": "string"
        }
      ],
      "reader_parallelism_num":1
    },
    "writer": {
      "class": "com.bytedance.bitsail.connector.legacy.print.sink.PrintSink"
    }
  }
}
```


## Writer Example

```json
{
  "job": {
    "common": {
      "job_id": -2314,
      "job_name": "bitsail_fake_to_hbase_test",
      "instance_id": -20314,
      "user_name": "root"
    },
    "reader": {
      "class": "com.bytedance.bitsail.connector.legacy.fake.source.FakeSource",
      "total_count": 300,
      "rate": 1000,
      "random_null_rate": 0,
      "columns": [
        {
          "index": 0,
          "name": "str1",
          "type": "string"
        },
        {
          "index": 1,
          "name": "int1",
          "type": "bigint"
        },
        {
          "index": 2,
          "name": "str2",
          "type": "string"
        },
        {
          "index": 3,
          "name": "int3",
          "type": "bigint"
        }
      ]
    },
    "writer": {
      "class": "com.bytedance.bitsail.connector.hbase.sink.HBaseOutputFormat",
      "table": "test_table",
      "hbase_conf":{
        "hbase.zookeeper.quorum":"127.0.0.1",
        "hbase.zookeeper.property.clientPort":"2181"
      },
      "row_key_column": "id_$(cf1:str1)",
      "columns": [
        {
          "index": 0,
          "name": "cf1:str1",
          "type": "string"
        },
        {
          "index": 1,
          "name": "cf1:int1",
          "type": "bigint"
        },
        {
          "index": 2,
          "name": "cf2:str2",
          "type": "string"
        },
        {
          "index": 3,
          "name": "cf3:int3",
          "type": "bigint"
        }
      ]
    }
  }
}
```