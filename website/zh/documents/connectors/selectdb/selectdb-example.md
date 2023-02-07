# Selectdb 连接器配置示例

父目录: [selectdb-connector](./selectdb.md)

## 测试用Selectdb配置

假设 Selectdb 集群配置如下:

- cluster: test_cluster
- load_url: `<selectdb url>:<http port>`
- jdbc_url: `<selectdb url>:<mysql port>`

账户信息为:

- 用户: `admin`
- 密码: `password`

要写入的库表为:

- table_identifier: test_db.test_selectdb_table

该表的建表语句为:

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

## Selectdb 写连接器

可用下面的配置写入`test_db.test_selectdb_table`表:

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