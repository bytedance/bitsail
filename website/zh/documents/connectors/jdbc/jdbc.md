# JDBC 连接器

上级文档：[连接器](../README.md)

Jdbc Connector 通过 JDBC 直连数据库，通过批式的方式，将数据导入到其他存储或者将其他存储的数据导入到数据库中。JDBC connectors 读取 slaves 以最小化对数据库的影响。

目前支持读取和写入 MySQL、Oracle、PgSQL、SqlServer 四种数据源。

## 支持的数据类型

MySQL 支持以下数据类型

* bit
* tinyint
* tinyint unsigned
* smallint
* smallint unsigned
* mediumint
* mediumint unsigned
* enum
* int
* int unsigned
* bigint
* bigint unsigned
* timestamp
* datetime
* float
* float unsigned
* double
* double unsigned
* decimal
* decimal unsigned
* real
* date
* time
* year
* char
* varchar
* longvarchar
* nvar
* nvarchar
* longnvarchar
* tinytext
* mediumtext
* longtext
* text
* string
* json
* blob
* mediumblob
* tinyblob
* longblob
* binary
* longvarbinary
* varbinary
* geometry
* multipolygon
* set

### Oracle 支持以下数据类型
* char
* varchar
* interval day
* interval year
* intervalds
* intervalym
* varchar2
* nchar
* nvarchar2
* long
* blob
* clob
* nclob
* string
* character
* number
* integer
* int
* smallint
* float
* double
* double precision
* numeric
* decimal
* real
* bit
* bool
* date
* timestamp
* timestamp with time zone
* timestamp with local time zone
* datetime
* blob
* bfile
* raw
* long raw
* rowid
* urowid
* xmltype
* binary_float
* binary_double

PgSQL 支持以下数据类型

* char
* bpchar
* varchar
* text
* character varying
* string
* character
* bigint
* int8
* integer
* int
* int4
* smallserial
* serial
* bigserial
* smallint
* int2
* double
* money
* double precision
* float8
* numeric
* decimal
* real
* float
* float4
* boolean
* bool
* date
* time
* timetz
* timestamp
* timestamptz
* bytea
* bit
* bit varying
* varbit
* uuid
* cidr
* xml
* inet
* macaddr
* enum
* json
* jsonb
* aclitem
* _aclitem
* _int2
* _int4
* _float4
* _text
* _char
* cid
* inet
* int2vector
* interval
* oid
* _oid
* pg_node_tree

SqlServer 支持以下数据类型：

* char
* varchar
* text
* nchar
* nvarchar
* ntext
* bigint
* int
* int identity
* integer
* smallint
* tinyint
* float
* double precision
* numeric
* decimal
* money
* real
* bit
* date
* timestamp
* datetime
* datetime2
* time
* binary
* varbinary
* image
* datetimeoffset
* smalldatetime
* sql_variant
* uniqueidentifier
* xml

## Jdbc Source

> 在使用MySQL相关功能时，需要再连接参数中增加`permitMysqlScheme`选项。

### 主要功能

* 支持多种分片算法
* 支持分库分表库的读取
* 支持表同步和 SQL 同步
* 支持过滤语句

### 主要参数

通用参数

| 参数名称    | 参数默认值 | 参数是否必须 | 参数类型                     | 建议值 or 示例值                                                               | 参数含义                                    |
|---------|-------|--------|--------------------------|--------------------------------------------------------------------------|-----------------------------------------|
| class   | -     | 是      | string                   | com.bytedance.bitsail.connector.legacy.jdbc.source.JDBCInputFormat       | Mysql 读取 connector class 名称             |
| class   | -     | 是      | string                   | com.bytedance.bitsail.connector.legacy.jdbc.source.OracleInputFormat     | Oracle 读取 connector class 名称            |
| class   | -     | 是      | string                   | com.bytedance.bitsail.connector.legacy.jdbc.source.PostgresqlInputFormat | Pgsql 读取 connector class 名称             |
| class   | -     | 是      | string                   | com.bytedance.bitsail.connector.legacy.jdbc.source.SqlServerInputFormat  | SqlServer 读取 connector class 名称         |
| columns | -     | 否      | list<map<string,string>> | "[ { "name":"id", "type":"int" }, { "name":"name", "type":"varchar" } ]  | Jdbc 读取的列信息。需要和writer的指定的columns数量保持一致。 |

数据库连接配置

| 参数名称                  | 参数默认值 | 参数是否必须 | 参数类型   | 建议值 or 示例值                                                                                                                                                                                                                                                                             | 参数含义               |
|-----------------------|-------|--------|--------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------|
| user_name             | -     | 是      | string | abc                                                                                                                                                                                                                                                                                    | Jdbc 连接用户名         |
| password              | -     | 是      | string | password                                                                                                                                                                                                                                                                               | Jdbc 连接密码          |
| query_timeout_seconds | 300   | 否      | int    | 300                                                                                                                                                                                                                                                                                    | 连接 jdbc timeout 时间 |
| query_retry_times     | 3     | 否      | int    | 3                                                                                                                                                                                                                                                                                      | Jdbc 重试次数          |
| connections           | -     | 是      |        | [ { "slaves": [ {"db_url": "jdbc:mysql://address=(protocol=tcp)(host=192.168.1.202)(port=3306)/test?permitMysqlScheme&rewriteBatchedStatements=true&autoReconnect=true&useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&jdbcCompliantTruncation=false"} ]} ] | 连接 Jdbc 的信息        |

表同步配置参数

| 参数名称               | 参数默认值    | 参数是否必须 | 参数类型   | 建议值 or 示例值                   | 参数含义                                                                                                        |
|--------------------|----------|--------|--------|------------------------------|-------------------------------------------------------------------------------------------------------------|
| db_name            | -        | 是      | string | db                           | Jdbc 连接db 名                                                                                                 |
| table_schema       | -        | 否      | string | schema                       | Jdbc 连接Schema 名称，通常只用于 PgSql                                                                                |
| table_name         | -        | 表同步必须  | string | table                        | 同步的表名                                                                                                       |
| split_pk           | -        | 表同步必须  | string | id                           | 分片使用的主键                                                                                                     |
| split_pk_jdbc_type | int      | 否      | string | Int/String                   | 分片键字段类型，支持数字类型和字符串类型                                                                                        |
| shard_split_mode   | accurate | 否      | string | quick, accurate, parallelism | 分片方式，accurate 会确保每次只拉取 reader_fetch_size 条数据，分片比较均匀，担心分片会比较慢；parallelism 将所有数据分片按照并发数量进行分片，分片会比较快，但是可能分片不均匀 |
| reader_fetch_size  | 10000    | 否      | int    | 10000                        | 每次拉取数据条数                                                                                                    |

SQL 同步配置参数

| 参数名称           | 参数默认值 | 参数是否必须   | 参数类型   | 建议值 or 示例值                              | 参数含义       |
|----------------|-------|----------|--------|-----------------------------------------|------------|
| customized_sql | -     | SQL 同步必须 | string | Select id,name from xx.xx where id > 10 | 自定义拉取SQL语句 |

其他配置

| 参数名称   | 参数默认值 | 参数是否必须 | 参数类型   | 建议值 or 示例值 | 参数含义                           |
|--------|-------|--------|--------|------------|--------------------------------|
| filter | -     | 否      | string | id>100     | 读取数据时过滤信息，会通过 where 语句放置在查询语句后 |

## Jdbc Sink

> 在使用MySQL相关功能时，需要再连接参数中增加`permitMysqlScheme`选项。

### 主要功能

* 支持 TTL 清理，在执行导入任务前会按照用户配置的TTL参数删除过期数据，默认TTL为0，即数据永久有效。
* 支持多种写入模式：清除式写入模式和覆盖式写入模式
  * 清除式写入: 需要有时间分区字段，写入时，若时间分区已存在，清除已有时间分区数据，再进行写入
  * 覆盖式写入: 不需要时间分区字段，写入时，不清除数据，按照唯一键upsert，用新的数据覆盖旧数据。当写入出现duplicate key的时候，会进行on duplicate key update操作，来更新字段。另外注意分库分表不支持更新分片建，需要配置job.writer.shard_key参数，value为分片建，多个分片建以逗号分隔

### 主要参数

通用参数

| 参数名称    | 参数默认值 | 参数是否必须 | 参数类型                     | 建议值 or 示例值                                                                | 参数含义                                    |
|---------|-------|--------|--------------------------|---------------------------------------------------------------------------|-----------------------------------------|
| class   | -     | 是      | string                   | com.bytedance.bitsail.connector.legacy.jdbc.source.JDBCOutputFormat       | Mysql 写入 connector class 名称             |
| class   | -     | 是      | string                   | com.bytedance.bitsail.connector.legacy.jdbc.source.OracleOutputFormat     | Oracle 写入 connector class 名称            |
| class   | -     | 是      | string                   | com.bytedance.bitsail.connector.legacy.jdbc.source.PostgresqlOutputFormat | Pgsql 写入 connector class 名称             |
| class   | -     | 是      | string                   | com.bytedance.bitsail.connector.legacy.jdbc.source.SqlServerOutputFormat  | SqlServer 写入 connector class 名称         |
| columns | -     | 是      | list<map<string,string>> | "[ { "name":"id", "type":"int" }, { "name":"name", "type":"varchar" } ]   | Jdbc 写入的列信息。需要和reader的指定的columns数量保持一致。 |

数据库连接配置

| 参数名称         | 参数默认值                             | 参数是否必须 | 参数类型   | 建议值 or 示例值                                                                                                                                                                                                                                            | 参数含义                         |
|--------------|-----------------------------------|--------|--------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------|
| user_name    | -                                 | 是      | string | abc                                                                                                                                                                                                                                                   | Jdbc 连接用户名                   |
| password     | -                                 | 是      | string | password                                                                                                                                                                                                                                              | Jdbc 连接密码                    |
| connections  | -                                 | 是      |        | [ { "db_url": "jdbc:mysql://address=(protocol=tcp)(host=192.168.1.202)(port=3306)/test?rewriteBatchedStatements=true&autoReconnect=true&useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&jdbcCompliantTruncation=false" } ] | 连接 Jdbc 的信息                  |
| db_name      | -                                 | 是      | string | db                                                                                                                                                                                                                                                    | Jdbc 连接db 名                  |
| table_schema | PgSQL 默认为public； Sql server默认为dbo | 否      | string | schema                                                                                                                                                                                                                                                | Jdbc 连接Schema 名称，通常只用于 PgSql |
| table_name   | -                                 | 是      | string | table                                                                                                                                                                                                                                                 | 同步的表名                        |

写入模式配置参数

| 参数名称       | 参数默认值  | 参数是否必须 | 参数类型   | 建议值 or 示例值 | 参数含义                                                                                                         |
|------------|--------|--------|--------|------------|--------------------------------------------------------------------------------------------------------------|
| write_mode | insert | 否      | string | insert     | Insert 写入模式。为了保证重复执行结果的一致性。写入前会根据分区列清除数据。最终生成的写入语句类似INSERT INTO xx (xx) VALUES (xx)                          |
| write_mode |        |        |        | overwrite  | Overwrite 写入模式。写入前不会清除数据。最终生成的写入语句类似 INSERT INTO xx (xx) VALUES (xx) ON DUPLICATE KEY UPDATE (xx) VALUES(xx) |

Insert 模式下会根据 partition 信息进行数据删除，下面的参数针对 insert 模式：

| 参数名称                     | 参数默认值 | 参数是否必须 | 参数类型   | 建议值 or 示例值          | 参数含义                                                                                                                                           |
|--------------------------|-------|--------|--------|---------------------|------------------------------------------------------------------------------------------------------------------------------------------------|
| partition_name           | -     | 是      | string | date                | 分区名称，这是一个逻辑概念，写入数据前会根据该字段删除partition value的数据                                                                                                  |
| partition_value          | -     | 是      | string | 20220727            | 分区值                                                                                                                                            |
| partition_pattern_format | -     | 否      | string | yyyyMMdd/yyyy-MM-dd | 分区字段模式                                                                                                                                         |
| mysql_data_ttl           | 0     | 否      | int    | 0                   | 数据库数据保存的天数。会根据配置的ddl 和 partition_name 字段的值进行删除操作。比如 ttl 设置为3，partition name 为 date，partition value 设置为 20220727，则会将数据库中所有 date<=20220724 的数据删除 |
| delete_threshold         | 10000 | 否      | int    | 10000               | 删除时，每次删除数据的条数                                                                                                                                  |
| delete_interval_ms       | 100   | 否      | int    | 100                 | 两次删除之间的间隔                                                                                                                                      |
写入配置信息

| 参数名称                   | 参数默认值 | 参数是否必须 | 参数类型 | 建议值 or 示例值 | 参数含义       |
|------------------------|-------|--------|------|------------|------------|
| write_batch_interval   | 100   | 否      | int  | 100        | 写入batch 大小 |
| write_retry_times      | 3     | 否      | int  | 3          | 写入时重试次数    |
| retry_interval_seconds | 10    | 否      | int  | 10         | 两次重试间的间隔   |

其他配置

| 参数名称         | 参数默认值 | 参数是否必须   | 参数类型   | 建议值 or 示例值 | 参数含义             |
|--------------|-------|----------|--------|------------|------------------|
| pre_query    | -     | 否        | string | Select 1   | 连接数据库后最先执行的语句    |
| verify_query | -     | 否        | string | Select 1   | 任务运行后执行的校验语句     |
| shard_key    | -     | 否（分片库必须） | string | id         | 分片库的分片键，非分片库不用配置 |

PgSql 写入的定制参数

| 参数名称                     | 参数默认值 | 参数是否必须 | 参数类型   | 建议值 or 示例值 | 参数含义                                                 |
|--------------------------|-------|--------|--------|------------|------------------------------------------------------|
| primary_key              | -     | 否      | string | id         | 表的主键，pgSQL 删除时如果需要限制速率需要利用主键值使用 select limit语句限制删除速率 |
| upsert_key               | -     | 否      | string | id         | 唯一性索引，支持覆盖写，PG只支持对单个唯一性索引做覆盖写                        |
| delete_threshold_enabled | TRUE  | 否      | string | Truefalse  | 是否需要限制删除速率，默认为true，false时不需要提供primary key            |
| is_truncate_mode         | FALSE | 否      | string | Truefalse  | 是否为truncate模式，true会先删除全表同时不需要分区列；非truncate模式需要有分区列   |

Oracle 写入的定制参数

| 参数名称           | 参数默认值 | 参数是否必须   | 参数类型           | 建议值 or 示例值 | 参数含义                                                  |
|----------------|-------|----------|----------------|------------|-------------------------------------------------------|
| primary_key    | -     | 是 (Sink) | string（区分大小写）  | ID         | 表的主键，Oracle 删除时如果需要限制速率需要利用主键值使用 select limit语句限制删除速率 |
| partition_name | -     | 是        | string (区分大小写) | DATETIME   | 跟通用参数相同，除了数值必須区分大小写                                   |
| db_name        | -     | 是        | string (区分大小写) | DB         | 跟通用参数相同，除了数值必須区分大小写                                   |
| columns.name   | -     | 否        | string (区分大小写) | COLUMN     | 跟通用参数相同，除了数值必須区分大小写                                   |

## 相关文档

配置示例文档：[JDBC 连接器示例](./jdbc-example.md).
