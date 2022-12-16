# Hadoop connector examples

Parent document: [hadoop-connector](./hadoop.md)

The following configuration shows how to organize parameter configuration to read the following json format hdfs file.

- Example json data
```json
{"id":0,"string_type":"test_string","map_string_string":{"k1":"v1","k2":"v2","k3":"v3"},"array_string":["a1","a2","a3","a4"]}
```


- The `local Hadoop environment` is used for testing,  and the sample json data is uploaded to the `/test_namespace/source directory` of hdfs
- Configuration file used to read the above hdfs file:

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
      "defaultFS": "hdfs://127.0.0.1:9000/",
      "path_list": "/test_namespace/source/test.json",
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

The following configuration shows how to organize parameter configuration to read the following csv format hdfs file.

- Example csv data

```csv
1,100001,100.001,text_0001,2020-01-01
2,100002,100.002,text_0002,2020-01-02
```


- The `local Hadoop environment` is used for testing,  and the sample json data is uploaded to the `/test_namespace/source directory` of hdfs
- Configuration file used to read the above hdfs file:

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
      "defaultFS": "hdfs://127.0.0.1:9000/",
      "path_list": "/test_namespace/source/test.csv",
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

