# Selectdb connector example

Parent documents: [selectdb-connector](./selectdb.md)

## Selectdb configuration for testing

Selectdb cluster info

- cluster: test_cluster
- load_url: `<selectdb url>:<http port>`
- jdbc_url: `<selectdb url>:<mysql port>`

Account:

- User: `admin`
- Password: `password`

Target database and table:

- table_identifier: test_db.test_selectdb_table

DDL statement is:

```sql
CREATE TABLE `test_db.test_selectdb_table`
(
    `id`             bigint(20) NULL,
    `bigint_type`    bigint(20) NULL,
    `string_type`    varchar(100) NULL,
    `double_type`    double NULL,
    `decimal_type`   decimal(27, 9) NULL,
    `date_type`      date NULL,
    `partition_date` date NULL
) ENGINE=OLAP
DUPLICATE KEY(`id`)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(`id`) BUCKETS 10;
```

## Selectdb writer

You can use the following configuration to write data for table test_db.test_selectdb_table.

```json
{
  "job": {
    "writer": {
      "class": "com.bytedance.bitsail.connector.selectdb.sink.SelectdbSink",
      "load_url": "<your selectdb http hosts>",
      "jdbc_url": "<your selectdb mysql hosts>",
      "cluster_name": "<selectdb cluster name>",
      "user": "<user name>",
      "password": "<password>",
      "table_identifier": "<selectdb table identifier, like: test_db.test_selectdb_table>",
      "columns": [
        {
          "index": 0,
          "name": "id",
          "type": "bigint"
        },
        {
          "index": 1,
          "name": "bigint_type",
          "type": "bigint"
        },
        {
          "index": 2,
          "name": "string_type",
          "type": "varchar"
        },
        {
          "index": 3,
          "name": "double_type",
          "type": "double"
        },
        {
          "index": 4,
          "name": "decimal_type",
          "type": "double"
        },
        {
          "index": 5,
          "name": "date_type",
          "type": "date"
        },
        {
          "index": 6,
          "name": "partition_date",
          "type": "date"
        }
      ]
    }
  }
}
```