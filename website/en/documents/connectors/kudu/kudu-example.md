# Kudu connector example

Parent document: [Kudu connector](./kudu.md)

## Kudu reader example

Assuming there is a test kudu cluster with master address "127.0.0.1:64086", then we can use the following configuration to read `test_kudu_table` table.

```json
{
  "job":{
    "common":{
      "instance_id":3124,
      "job_name":"bitsail_kudu_to_print_test"
    },
    "reader":{
      "kudu_table_name":"test_kudu_table",
      "kudu_master_address_list":[
        "127.0.0.1:64086"
      ],
      "read_mode":"READ_LATEST",
      "columns":[
        {
          "name":"key",
          "type":"int64"
        },
        {
          "name":"field_boolean",
          "type":"boolean"
        },
        {
          "name":"field_int",
          "type":"int"
        },
        {
          "name":"field_double",
          "type":"double"
        },
        {
          "name":"field_date",
          "type":"date"
        },
        {
          "name":"field_string",
          "type":"string"
        },
        {
          "name":"field_binary",
          "type":"binary"
        }
      ],
      "predicates": "[\"AND\", [\">=\", \"key\", 1000], [\"IN\", \"key\", [999, 1001, 1003, 1005, 1007, 1009]], [\"NULL\", \"field_varchar\"], [\"NOTNULL\",\"field_binary\"]]",
      "class":"com.bytedance.bitsail.connector.kudu.source.KuduSource"
    },
    "writer":{
      "class":"com.bytedance.bitsail.connector.legacy.print.sink.PrintSink"
    }
  }
}
```


## Kudu writer example


Assuming there is a test kudu cluster with master address "127.0.0.1:64086", then we can use the following configuration to write into `test_kudu_table` table.

```json
{
  "job": {
    "common": {
      "job_id": -2413,
      "job_name": "bitsail_fake_to_kudu_test",
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
          "name": "key",
          "type": "long",
          "properties": "unique"
        },
        {
          "name": "fake_boolean",
          "type": "boolean"
        },
        {
          "name": "fake_int",
          "type": "int"
        },
        {
          "name": "fake_double",
          "type": "double"
        },
        {
          "name": "fake_date",
          "type": "date.date"
        },
        {
          "name": "fake_string",
          "type": "string"
        },
        {
          "name": "fake_binary",
          "type": "binary"
        }
      ]
    },
    "writer": {
      "class": "com.bytedance.bitsail.connector.kudu.sink.KuduSink",
      "kudu_worker_count": 2,
      "kudu_table_name":"test_kudu_table",
      "kudu_master_address_list":[
        "127.0.0.1:64086"
      ],
      "columns": [
        {
          "name": "key",
          "type": "long",
          "properties": "unique"
        },
        {
          "name": "field_boolean",
          "type": "boolean"
        },
        {
          "name": "field_int",
          "type": "int"
        },
        {
          "name": "field_double",
          "type": "double"
        },
        {
          "name": "field_date",
          "type": "date"
        },
        {
          "name": "field_string",
          "type": "string",
          "properties": "nullable"
        },
        {
          "name": "field_binary",
          "type": "binary",
          "properties": "nullable"
        }
      ]
    }
  }
}
```