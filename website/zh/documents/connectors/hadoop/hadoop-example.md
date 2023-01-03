# Hadoop 连接器示例

上级文档：[Hadoop 连接器](./hadoop.md)

下面展示了如何使用用户参数配置读取如下json格式hdfs文件。

- 示例json数据
```json
{"id":0,"string_type":"test_string","map_string_string":{"k1":"v1","k2":"v2","k3":"v3"},"array_string":["a1","a2","a3","a4"]}
```


- 用于读取上述格式hdfs文件的配置

```json
{
  "job": {
    "reader": {
      "class": "com.bytedance.bitsail.connector.hadoop.source.HadoopInputFormat",
      "path_list": "hdfs://test_namespace/test.json",
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