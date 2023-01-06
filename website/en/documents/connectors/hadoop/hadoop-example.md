# Hadoop connector example

Parent document: [Hadoop connector](./hadoop.md)

The following configuration shows how to organize parameter configuration to read the following json format hdfs file.

- Example json data
```json
{"id":0,"string_type":"test_string","map_string_string":{"k1":"v1","k2":"v2","k3":"v3"},"array_string":["a1","a2","a3","a4"]}
```


- Configuration file used to read the above hdfs file:

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