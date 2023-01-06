# Druid 连接器示例

父目录：[Druid 连接器](./druid.md)

## Druid 写连接器

假设当前有一个测试Druid集群，其master地址为"127.0.0.1:64086"，则可以通过如下配置写入test_druid_datasource资料源。

```json
{
  "job": {
    "common": {
      "job_id": -2413,
      "job_name": "bitsail_fake_to_druid_test",
      "instance_id": -20413,
      "user_name": "user"
    },
    "reader": {
      "class": "com.bytedance.bitsail.connector.fake.source.FakeSource",
      "total_count": 5000,
      "rate": 1000,
      "null_percentage": 20,
      "columns": [
        {
          "index": 0,
          "name": "string_type",
          "type": "string"
        },
        {
          "index": 1,
          "name": "int_type",
          "type": "int"
        },
        {
          "index": 2,
          "name": "long_type",
          "type": "long"
        },
        {
          "index": 3,
          "name": "float_type",
          "type": "float"
        },
        {
          "index": 4,
          "name": "double_type",
          "type": "double"
        }
      ]
    },
    "writer": {
      "class": "com.bytedance.bitsail.connector.kudu.sink.KuduSink",
      "datasource": "test_druid_datasource",
      "coordinator_url": "127.0.0.1:64086",
      "columns": [
        {
          "index": 0,
          "name": "string_type",
          "type": "string"
        },
        {
          "index": 1,
          "name": "int_type",
          "type": "int"
        },
        {
          "index": 2,
          "name": "long_type",
          "type": "long"
        },
        {
          "index": 3,
          "name": "float_type",
          "type": "float"
        },
        {
          "index": 4,
          "name": "double_type",
          "type": "double"
        }
      ]
    }
  }
}
```