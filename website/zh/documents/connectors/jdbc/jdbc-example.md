# JDBC 连接器示例

上级文档：[JDBC 连接器](./jdbc.md)

## MySQL Example

### MySQL source

#### MySQL table sync

```json
{
    "job":{
        "reader":{
            "class":"com.bytedance.bitsail.connector.legacy.jdbc.source.JDBCInputFormat",
            "columns":[
                {
                    "name":"id",
                    "type":"bigint"
                },
                {
                    "name":"name",
                    "type":"varchar"
                },
                {
                    "name":"int_info",
                    "type":"int"
                },
                {
                    "name":"double_info",
                    "type":"double"
                },
                {
                    "name":"bytes_info",
                    "type":"binary"
                }
            ],
            "table_name":"your table name",
            "password":"your password",
            "db_name":"your db name",
            "user_name":"your user name",
            "split_pk":"id",
            "connections":[
                {
                    "slaves":[
                        {
                            "db_url":"jdbc:mysql://address=(protocol=tcp)(host=192.168.1.202)(port=3306)/test?permitMysqlScheme&rewriteBatchedStatements=true&autoReconnect=true&useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&jdbcCompliantTruncation=false"
                        }
                    ]
                }
            ]
        }
    }
}
```

#### MySQL SQL sync

```json
{
    "job":{
        "reader":{
            "class":"com.bytedance.bitsail.connector.legacy.jdbc.source.JDBCInputFormat",
            "columns":[
                {
                    "index":0,
                    "name":"id",
                    "type":"bigint"
                },
                {
                    "name":"name",
                    "type":"varchar"
                },
                {
                    "name":"int_info",
                    "type":"int"
                },
                {
                    "name":"double_info",
                    "type":"double"
                },
                {
                    "name":"bytes_info",
                    "type":"binary"
                }
            ],
            "password":"your password",
            "db_name":"your db name",
            "user_name":"your user name",
            "customized_sql":"select id, name, int_info, double_info, bytes_info from table_name where id < 100",
            "connections":[
                {
                    "slaves":[
                        {
                            "db_url":"jdbc:mysql://address=(protocol=tcp)(host=192.168.1.202)(port=3306)/test?permitMysqlScheme&rewriteBatchedStatements=true&autoReconnect=true&useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&jdbcCompliantTruncation=false"
                        }
                    ]
                }
            ]
        }
    }
}
```

### MySQL sink

#### MySQL insert sync

```json
{
    "job":{
        "writer":{
            "class":"com.bytedance.bitsail.connector.legacy.jdbc.sink.JDBCOutputFormat",
            "partition_value":"20221001",
            "user_name":"test",
            "columns":[
                {
                    "name":"name",
                    "type":"varchar"
                },
                {
                    "name":"int_info",
                    "type":"int"
                },
                {
                    "name":"double_info",
                    "type":"double"
                },
                {
                    "name":"bytes_info",
                    "type":"binary"
                }
            ],
            "partition_pattern_format":"yyyyMMdd",
            "table_name":"table name",
            "password":"password",
            "db_name":"test",
            "connections":[
                {
                    "db_url":"jdbc:mysql://address=(protocol=tcp)(host=192.168.1.202)(port=3306)/test?permitMysqlScheme&rewriteBatchedStatements=true&autoReconnect=true&useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&jdbcCompliantTruncation=false"
                }
            ],
            "partition_name":"datetime"
        }
    }
}
```

#### MySQL overwrite sync

```json
{
    "job":{
        "writer":{
            "class":"com.bytedance.bitsail.connector.legacy.jdbc.sink.JDBCOutputFormat",
            "write_mode":"overwrite",
            "user_name":"test",
            "columns":[
                {
                    "name":"name",
                    "type":"varchar"
                },
                {
                    "name":"int_info",
                    "type":"int"
                },
                {
                    "name":"double_info",
                    "type":"double"
                },
                {
                    "name":"bytes_info",
                    "type":"binary"
                }
            ],
            "table_name":"table name",
            "password":"password",
            "db_name":"test",
            "connections":[
                {
                    "db_url":"jdbc:mysql://address=(protocol=tcp)(host=192.168.1.202)(port=3306)/test?permitMysqlScheme&rewriteBatchedStatements=true&autoReconnect=true&useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&jdbcCompliantTruncation=false"
                }
            ]
        }
    }
}
```

-----

## Oracle Example

### Oracle source

```json
{
    "job":{
        "reader":{
            "class":"com.bytedance.bitsail.connector.legacy.jdbc.source.OracleInputFormat",
            "columns": [
                {
                  "index": 0,
                  "name": "ID",
                  "type": "varchar"
                },
                {
                  "index": 1,
                  "name": "INT_TYPE",
                  "type": "integer"
                },
                {
                  "index": 2,
                  "name": "BIGINT_TYPE",
                  "type": "number"
                },
                {
                  "index": 3,
                  "name": "FLOAT_TYPE",
                  "type": "float"
                },
                {
                  "index": 4,
                  "name": "DOUBLE_TYPE",
                  "type": "double"
                },
                {
                  "index": 5,
                  "name": "RAW_TYPE",
                  "type": "raw"
                },
                {
                  "index": 6,
                  "name": "DATE_TYPE",
                  "type": "date"
                }
            ],
            "user_name":"your user name",
            "password":"your password",
            "db_name":"your db name",
            "table_schema":"your schema name",
            "table_name":"your table name",
            "split_pk":"ID",
            "connections":[
                {
                    "slaves":[
                        {
							"db_url":"jdbc:oracle:thin:@localhost:51912/TEST",
							"host":"localhost",
							"port":51912
						}
                    ]
                }
            ]
        }
    }
}
```

### Oracle sink

```json
{
    "job":{
        "writer": {
          "class": "com.bytedance.bitsail.connector.legacy.jdbc.sink.OracleOutputFormat",
          "db_name": "your db name",
          "table_name": "your table name",
          "primary_key": "ID",
          "connections": [
          ],
          "user_name": "your user name",
          "password": "your password",
          "write_mode": "insert",
          "writer_parallelism_num": 1,
          "partition_name": "DATETIME",
          "partition_value": "20220705",
          "partition_pattern_format": "yyyyMMdd",
          "columns": [
            {
              "index": 0,
              "name": "ID",
              "type": "varchar"
            },
            {
              "index": 1,
              "name": "INT_TYPE",
              "type": "integer"
            },
            {
              "index": 2,
              "name": "BIGINT_TYPE",
              "type": "number"
            },
            {
              "index": 3,
              "name": "FLOAT_TYPE",
              "type": "float"
            },
            {
              "index": 4,
              "name": "DOUBLE_TYPE",
              "type": "double"
            },
            {
              "index": 5,
              "name": "RAW_TYPE",
              "type": "raw"
            },
            {
              "index": 6,
              "name": "DATE_TYPE",
              "type": "date"
            }
          ],
          "connections":[
            {
              "db_url":"jdbc:oracle:thin:@localhost:1521/test?currentSchema=opensource_test&rewriteBatchedStatements=true&autoReconnect=true&useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull"
            }
          ]
        }
    }
}
```

-----

## PostgreSQL Example

### PostgreSQL source

```json
{
    "job":{
        "reader":{
            "class":"com.bytedance.bitsail.connector.legacy.jdbc.source.PostgresqlInputFormat",
            "columns":[
                {
                    "index":0,
                    "name":"id",
                    "type":"bigint"
                },
                {
                    "name":"name",
                    "type":"varchar"
                },
                {
                    "name":"int_info",
                    "type":"int"
                },
                {
                    "name":"double_info",
                    "type":"double"
                },
                {
                    "name":"bytes_info",
                    "type":"bytea"
                }
            ],
            "user_name":"your user name",
            "password":"your password",
            "db_name":"your db name",
            "table_schema":"your schema name",
            "table_name":"your table name",
            "split_pk":"id",
            "connections":[
                {
                    "slaves":[
                        {
                            "db_url":"jdbc:postgresql://192.168.1.202:5432/test?currentSchema=opensource_test&rewriteBatchedStatements=true&autoReconnect=true&useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull"
                        }
                    ]
                }
            ]
        }
    }
}
```

### PostgreSQL sink

```json
{
    "job":{
        "writer":{
            "class":"com.bytedance.bitsail.connector.legacy.jdbc.sink.PostgresqlOutputFormat",
            "partition_value":"20221001",
            "user_name":"test",
            "columns":[
                {
                    "name":"name",
                    "type":"varchar"
                },
                {
                    "name":"int_info",
                    "type":"int"
                },
                {
                    "name":"double_info",
                    "type":"double"
                },
                {
                    "name":"bytes_info",
                    "type":"binary"
                }
            ],
            "partition_pattern_format":"yyyyMMdd",
            "table_name":"table name",
            "password":"password",
            "db_name":"test",
            "table_schema":"your table schema",
            "primary_key": "id",
            "connections":[
                {
                    "db_url":"jdbc:postgresql://192.168.1.202:5432/test?currentSchema=opensource_test&rewriteBatchedStatements=true&autoReconnect=true&useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull"
                }
            ],
            "partition_name":"datetime"
        }
    }
}
```

-----

## SqlServer Example

### SqlServer source

```json
{
    "job":{
        "reader":{
            "class":"com.bytedance.bitsail.connector.legacy.jdbc.source.SqlServerInputFormat",
            "columns":[
                {
                    "index":0,
                    "name":"id",
                    "type":"bigint"
                },
                {
                    "name":"name",
                    "type":"varchar"
                },
                {
                    "name":"int_info",
                    "type":"int"
                },
                {
                    "name":"double_info",
                    "type":"double"
                }
            ],
            "table_name":"your table name",
            "password":"your password",
            "db_name":"your db name",
            "table_schema":"your table schema",
            "user_name":"your user name",
            "split_pk":"id",
            "connections":[
                {
                    "slaves":[
                        {
                            "db_url":"jdbc:sqlserver://192.168.1.202:1433;databaseName=dts_test"
                        }
                    ]
                }
            ]
        }
    }
}
```

### SqlServer sink

```json
{
    "job":{
        "writer":{
            "class":"com.bytedance.bitsail.connector.legacy.jdbc.sink.SqlServerOutputFormat",
            "partition_value":"20221001",
            "user_name":"test",
            "columns":[
                {
                    "name":"name",
                    "type":"varchar"
                },
                {
                    "name":"int_info",
                    "type":"int"
                },
                {
                    "name":"double_info",
                    "type":"double"
                }
            ],
            "partition_pattern_format":"yyyyMMdd",
            "table_name":"table name",
            "password":"password",
            "db_name":"test",
            "table_schema":"your table schema",
            "connections":[
                {
                    "db_url":"jdbc:sqlserver://192.168.1.202:1433;databaseName=dts_test"
                }
            ],
            "partition_name":"datetime"
        }
    }
}
```
