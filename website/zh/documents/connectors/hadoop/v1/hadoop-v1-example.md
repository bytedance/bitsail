# Hadoop连接器使用示例

上级文档: [hadoop连接器](./hadoop-v1.md)

下面展示了如何使用用户参数配置读取如下json格式hdfs文件。

- 示例json数据
```json
{"id":0,"string_type":"test_string","map_string_string":{"k1":"v1","k2":"v2","k3":"v3"},"array_string":["a1","a2","a3","a4"]}
```


- 测试时使用本地Hadoop环境，需自行配置，将示例json数据上传至hdfs的/test_namespace/source目录下
- 用于读取上述格式hdfs文件的配置

```json
{
  "job": {
    "common": {
      "job_id": 313,
      "instance_id": 3123,
      "job_name": "bitsail_hadoop_to_print_test",
      "user_name": "root"
    },
    "reader": {
      "class": "com.bytedance.bitsail.connector.hadoop.source.HadoopSource",
      "content_type":"json",
      "reader_parallelism_num": 1,
      "columns": [
        {
          "name":"id",
          "type": "int"
        },
        {
          "name": "string_type",
          "type": "string"
        },
        {
          "name": "map_string_string",
          "type": "map<string,string>"
        },
        {
          "name": "array_string",
          "type": "list<string>"
        }
      ]
    }
  }
}
```

下面展示了如何使用用户参数配置读取如下csv格式hdfs文件。

- 示例csv数据

```csv
1,100001,100.001,text_0001,2020-01-01
2,100002,100.002,text_0002,2020-01-02
```


- 测试时使用本地Hadoop环境，需自行配置，将示例json数据上传至hdfs的/test_namespace/source目录下
- 用于读取上述格式hdfs文件的配置

```json
{
  "job": {
    "common": {
      "job_id": 313,
      "instance_id": 3123,
      "job_name": "bitsail_hadoop_to_print_test",
      "user_name": "root"
    },
    "reader": {
      "class": "com.bytedance.bitsail.connector.hadoop.source.HadoopSource",
      "content_type":"csv",
      "reader_parallelism_num": 1,
      "columns": [
        {
          "name": "id",
          "type": "long"
        },
        {
          "name": "int_type",
          "type": "int"
        },
        {
          "name": "double_type",
          "type": "double"
        },
        {
          "name": "string_type",
          "type": "string"
        },
        {
          "name": "p_date",
          "type": "date"
        }
      ]
    }
  }
}
```

