# JDBC Connector

Parent document: [Connectors](../README.md)

The JDBC Connector directly connects to the database through JDBC, and imports data into other storages or imports other stored data into the database in a batch manner. JDBC connectors internally read from slaves to minimize the impact on DB.

Currently, supports reading and writing three kinds of data sources including MySQL, Oracle, PgSQL, SqlServer.

## Supported data types

### Supported by MySQL

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

### Supported by Oracle
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

### Supported by PgSQL 

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

### Supported by SqlServer

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

## JDBC Source

> Please add property `permitMysqlScheme in connection url when use MySQL.

### Main function

 - Supports multiple sharding algorithms
 - Support the reading of sub-database and sub-table database
 - Support table synchronization and SQL synchronization
 - Support filter statement

### Parameters

#### General parameters

| Param name | Default value | Required | Parameter type           | Recommended value / Example value                                        | Description                                                                                                       |
|------------|---------------|----------|--------------------------|--------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------|
| class      | -             | Yes      | string                   | com.bytedance.bitsail.connector.legacy.jdbc.source.JDBCInputFormat       | Reader class name for mysql                                                                                       |
| class      | -             | Yes      | string                   | com.bytedance.bitsail.connector.legacy.jdbc.source.OracleInputFormat     | Reader class name for Oracle                                                                                      |
| class      | -             | Yes      | string                   | com.bytedance.bitsail.connector.legacy.jdbc.source.PostgresqlInputFormat | Reader class name for Pgsql                                                                                       |
| class      | -             | Yes      | string                   | com.bytedance.bitsail.connector.legacy.jdbc.source.SqlServerInputFormat  | Reader class name for  SqlServer                                                                                  |
| columns    | -             | No       | list<map<string,string>> | "[ { "name":"id", "type":"int" }, { "name":"name", "type":"varchar" } ]  | Describing fields' names and types. It needs to be consistent with the number of columns specified by the writer. |

#### Database connection configuration

| Param name            | Default value | Required | Parameter type | Recommended value / Example value                                                                                                                                                                                                                                                      | Description                    |
|-----------------------|---------------|----------|----------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------|
| user_name             | -             | Yes      | string         | abc                                                                                                                                                                                                                                                                                    | JDBC username                  |
| password              | -             | Yes      | string         | password                                                                                                                                                                                                                                                                               | JDBC password                  |
| query_timeout_seconds | 300           | No       | int            | 300                                                                                                                                                                                                                                                                                    | JDBC connection timeout (s)    |
| query_retry_times     | 3             | No       | int            | 3                                                                                                                                                                                                                                                                                      | Max retry times for JDBC query |
| connections           | -             | Yes      |                | [ { "slaves": [ {"db_url": "jdbc:mysql://address=(protocol=tcp)(host=192.168.1.202)(port=3306)/test?permitMysqlScheme&rewriteBatchedStatements=true&autoReconnect=true&useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&jdbcCompliantTruncation=false"} ]} ] | JDBC connection urls           |

#### Table synchronization configuration parameters


| Param name         | Default value | Required                                 | Parameter type | Recommended value / Example value | Description                                                                                                                                                                                                                           |
|--------------------|---------------|------------------------------------------|----------------|-----------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| db_name            | -             | Yes                                      | string         | db                                | JDBC connection database name                                                                                                                                                                                                         |
| table_schema       | -             | No                                       | string         | schema                            | JDBC connection schema name, usually only used for PgSql                                                                                                                                                                              |
| table_name         | -             | Necessary if using table synchronization | string         | table                             | Table to read                                                                                                                                                                                                                         |
| split_pk           | -             | Necessary if using table synchronization | string         | id                                | The primary key used by the shard                                                                                                                                                                                                     |
| split_pk_jdbc_type | int           | No                                       | string         | Int/String                        | Shard key field type, supports numeric and string types                                                                                                                                                                               |
| shard_split_mode   | accurate      | No                                       | string         | quick, accurate, parallelism      | Splitting mode<br>accurate: ensure that only `reader_fetch_size` if pulled from table in each request.<br>parallelism: Splitting all data according to the reader parallelism num. The splitting will be fast, but may be nonuniform. |                |
| reader_fetch_size  | 10000         | No                                       | int            | 10000                             | Number of data pulled each time                                                                                                                                                                                                       |

#### SQL Synchronization Configuration Parameters

| Param name     | Default value | Required                               | Parameter type | Recommended value / Example value       | Description                                      |
|----------------|---------------|----------------------------------------|----------------|-----------------------------------------|--------------------------------------------------|
| customized_sql | -             | Necessary if using SQL Synchronization | string         | Select id,name from xx.xx where id > 10 | Custom SQL Statement for pulling data from table |

#### Other parameters

| Param name | Default value | Required | Parameter type | Recommended value / Example value | Description                                                                                               |
|------------|---------------|----------|----------------|-----------------------------------|-----------------------------------------------------------------------------------------------------------|
| filter     | -             | No       | string         | id>100                            | Filter conditions when pulling data. Will be placed after the query statement through the where statement | |

----

## JDBC Sink

> Please add property `permitMysqlScheme in connection url when use MySQL.

### Main function

 - Supports TTL. 
    - Before executing the import task, expired data will be deleted according to the TTL parameter configured by the user. 
    - The default TTL is 0, that is, the data is permanently valid.
 - Supports multiple write modes: <b>clear</b> write mode and <b>overwrite</b> write mode
    - Clear write: A time partition field is required. When writing, if the time partition already exists, clear the existing time partition data, and then write.
    - Overwrite write: No time partition field is required. When writing, the data is not cleared. According to the unique key upsert, the old data is overwritten with the new data. When a duplicate key appears in the write, the on duplicate key update operation will be performed to update the field. In addition, note that sharding and sharding do not support updating shards. You need to configure the `job.writer.shard_key` parameter. The value is sharding. Multiple shards are separated by `','`.

### Parameters

#### General parameters

| Param name | Default value | Is necessary | Parameter type           | Recommended value / Example value                                         | Description                                                                                                       |
|------------|---------------|--------------|--------------------------|---------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------|
| class      | -             | Yes          | string                   | com.bytedance.bitsail.connector.legacy.jdbc.source.JDBCOutputFormat       | Writer class name for mysql                                                                                       |
| class      | -             | Yes          | string                   | com.bytedance.bitsail.connector.legacy.jdbc.source.OracleOutputFormat     | Writer class name for Oracle                                                                                      |
| class      | -             | Yes          | string                   | com.bytedance.bitsail.connector.legacy.jdbc.source.PostgresqlOutputFormat | Writer class name for Pgsql                                                                                       |
| class      | -             | Yes          | string                   | com.bytedance.bitsail.connector.legacy.jdbc.source.SqlServerOutputFormat  | Writer class name for SqlServer                                                                                   |
| columns    | -             | Yes          | list<map<string,string>> | "[ { "name":"id", "type":"int" }, { "name":"name", "type":"varchar" } ]   | Describing fields' names and types. It needs to be consistent with the number of columns specified by the reader. |

#### Database connection configuration

| Param name   | Default value                             | Is necessary | Parameter type | Recommended value / Example value                                                                                                                                                                                                                     | Description                                  |
|--------------|-------------------------------------------|--------------|----------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------|
| user_name    | -                                         | Yes          | string         | abc                                                                                                                                                                                                                                                   | JDBC username                                |
| password     | -                                         | Yes          | string         | password                                                                                                                                                                                                                                              | JDBC password                                |
| connections  | -                                         | Yes          |                | [ { "db_url": "jdbc:mysql://address=(protocol=tcp)(host=192.168.1.202)(port=3306)/test?rewriteBatchedStatements=true&autoReconnect=true&useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&jdbcCompliantTruncation=false" } ] | JDBC connection urls                         |
| db_name      | -                                         | Yes          | string         | db                                                                                                                                                                                                                                                    | Database to connect                          |
| table_schema | "public" for PgSql<br>"dbo" for Sqlserver | No           | string         | schema                                                                                                                                                                                                                                                | Schema to connect，usually used only in PgSql |
| table_name   | -                                         | Yes          | string         | table                                                                                                                                                                                                                                                 | Table to write                               |

#### Write Mode Configuration Parameter

| Param name | Default value | Is necessary | Parameter type | Recommended value / Example value | Description                                                                                                                                                                                                                        |
|------------|---------------|--------------|----------------|-----------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| write_mode | insert        | No           | string         | insert                            | Insert Write mode. In order to ensure the consistency of repeated execution results, data is cleared according to the partition column before writing. The resulting write statement is similar to INSERT INTO xx (xx) VALUES (xx) |
| write_mode |               |              |                | overwrite                         | Overwrite write mode. Data is not cleared before writing. The resulting write statement looks like INSERT INTO xx (xx) VALUES (xx) ON DUPLICATE KEY UPDATE (xx) VALUES(xx)                                                         |

In insert mode, data will be deleted according to partition information. The following parameters are for insert mode:

| Param name               | Default value | Is necessary | Parameter type | Recommended value / Example value | Description                                                                                                                                                                                                                                                                                                                         |
|--------------------------|---------------|--------------|----------------|-----------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| partition_name           | -             | Yes          | string         | date                              | Partition name, this is a logical concept, meaning the data of partition value will be deleted according to this field before writing data.                                                                                                                                                                                         |
| partition_value          | -             | Yes          | string         | 20220727                          | Partition value                                                                                                                                                                                                                                                                                                                     |
| partition_pattern_format | -             | No           | string         | yyyyMMdd/yyyy-MM-dd               | Partition Field format                                                                                                                                                                                                                                                                                                              |
| mysql_data_ttl           | 0             | No           | int            | 0                                 | The number of days that  data is kept in database. The delete operation will be performed according to the value of the configured ddl and partition_name fields.<br>For example, if ttl is set to 3, partition name is date, and partition value is set to 20220727, all data with date<=20220724 in the database will be deleted. |
| delete_threshold         | 10000         | No           | int            | 10000                             | When deleting, the number of pieces of data deleted each time                                                                                                                                                                                                                                                                       |                                                                                                                                        |
| delete_interval_ms       | 100           | No           | int            | 100                               | Interval between deletes                                                                                                                                                                                                                                                                                                            |
#### Parameters for batch write

| Param name             | Default value | Is necessary | Parameter type | Recommended value / Example value | Description                 |
|------------------------|---------------|--------------|----------------|-----------------------------------|-----------------------------|
| write_batch_interval   | 100           | No           | int            | 100                               | write batch interval        |
| write_retry_times      | 3             | No           | int            | 3                                 | max retry time when writing |
| retry_interval_seconds | 10            | No           | int            | 10                                | retry interval (s)          |

#### Other parameters

| Param name   | Default value | Is necessary                   | Parameter type | Recommended value / Example value | Description                                                                          |
|--------------|---------------|--------------------------------|----------------|-----------------------------------|--------------------------------------------------------------------------------------|
| pre_query    | -             | No                             | string         | Select 1                          | The first statement to execute after connecting to the database                      |
| verify_query | -             | No                             | string         | Select 1                          | Validation statement to be executed after the task runs                              |
| shard_key    | -             | Necessary for sharded database | string         | id                                | The shard key of the sharded database, no need to configure the non-sharded database |

#### Parameters for PgSql 

| Param name               | Default value | Is necessary | Parameter type | Recommended value / Example value | Description                                                                                                                                                                      |               
|--------------------------|---------------|--------------|----------------|-----------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| primary_key              | -             | No           | string         | id                                | The primary key of the table, if you need to limit the rate when pgSQL deletes, you need to use the primary key value to use the select limit statement to limit the delete rate |
| upsert_key               | -             | No           | string         | id                                | Unique index, supports overwriting, PG only supports overwriting for a single unique index                                                                                       |
| delete_threshold_enabled | TRUE          | No           | string         | Truefalse                         | Whether to limit the deletion rate, the default is true, when false, you do not need to provide the primary key                                                                  |
| is_truncate_mode         | FALSE         | No           | string         | Truefalse                         | Whether it is truncate mode, true will delete the whole table first and no partition column is required; non-truncate mode requires a partition column                           |

#### Parameters for Oracle

| Param name     | Default value | Is necessary | Parameter type          | Recommended value / Example value | Description                                                                                                                                                                       |               
|----------------|---------------|--------------|-------------------------|-----------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| primary_key    | -             | Yes in Sink  | string (case sensitive) | ID                                | The primary key of the table, if you need to limit the rate when Oracle deletes, you need to use the primary key value to use the select limit statement to limit the delete rate |
| partition_name | -             | Yes          | string (case sensitive) | DATETIME                          | Same as general parameters except value is case sensitive.                                                                                                                        |
| db_name        | -             | Yes          | string (case sensitive) | DB                                | Same as general parameters except value is case sensitive.                                                                                                                        |
| columns.name   | -             | No           | string (case sensitive) | COLUMN                            | Same as general parameters except value is case sensitive.                                                                                                                        |

## Related documents

Configuration examples: [JDBC connector example](./jdbc_example.md)
