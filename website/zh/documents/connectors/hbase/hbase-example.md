# Hbase 连接器示例

父目录：[HBase 连接器](./hbase.md)

## 读写的HBase配置示例

- 在本地使用docker启动一个HBase集群，则默认配置为：
```json
{
  "hbase.zookeeper.quorum":"127.0.0.1",
  "hbase.zookeeper.property.clientPort":"2181"
}
```

- 创建一张名为`test_table`的表，有三个column family:

```shell
create 'test_table', 'cf1', 'cf2', 'cf3'
```

## HBase 读连接器

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

## HBase 写连接器

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