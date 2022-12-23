# ClickHouse 连接器示例

父目录：[ClickHouse 连接器](./clickhouse.md)

## ClickHouse 配置

假设 ClickHouse 服务配置为：
 - JDBC URL：`jdbc:clickhouse://127.0.0.1:8123`

账户信息: 
 - 用户：default
 - 密码：1234567

要写入的库表:
 - 库名：default
 - 表名：test_ch_table

表的建表语句为:

```sql
CREATE TABLE IF NOT EXISTS `default`.`test_ch_table` ( 
    `id` Int64, 
    `int_type` Int32,
    `double_type` Float64, 
    `string_type` String,
    `p_date` Date
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(p_date)
PRIMARY KEY id
```

插入部分测试数据：

```sql
INSERT INTO `default`.`test_ch_table`
    (*)
VALUES
       (1, 100001, 100.001, 'text_0001', '2020-01-01'),
       (2, 100002, 100.002, 'text_0002', '2020-01-02')
```

## ClickHouse 读连接器配置

读取上述 ClickHouse 表的任务配置示例：

```json
{
   "job": {
      "reader": {
        "class": "com.bytedance.bitsail.connector.clickhouse.source.ClickhouseSource",
        "jdbc_url": "jdbc:clickhouse://127.0.0.1:8123",
        "user_name": "default",
        "password": "1234567",
        "db_name": "default",
        "table_name": "test_ch_table",
        "split_field": "id",
        "split_config": "{\"lower_bound\": 0, \"upper_bound\": 10000, \"split_num\": 3}",
        "sql_filter": "( id % 2 == 0 )",
        "columns": [
          {
            "name": "id",
            "type": "int64"
          },
          {
            "name": "int_type",
            "type": "int32"
          },
          {
            "name": "double_type",
            "type": "float64"
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
