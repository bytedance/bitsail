# ClickHouse connector example

Parent document: [ClickHouse connector](./clickhouse.md)

## ClickHouse configuration

Suppose the ClickHouse service is configured as:
- JDBC URL: `jdbc:clickhouse://127.0.0.1:8123`

Account information:
- Username: default
- Password: 1234567

Target database and table:
- Database name: default
- Table name: test_ch_table

The table creation statement is:

```sql
CREATE TABLE IF NOT EXISTS `default`.`test_ch_table` (
     `id` Int64,
     `int_type` Int32,
     `double_type` Float64,
     `string_type` String,
     `p_date` Date
)
ENGINE=MergeTree
PARTITION BY toYYYYMM(p_date)
PRIMARY KEY id
```

Insert some test data:

```sql
INSERT INTO `default`.`test_ch_table`
     (*)
VALUES
        (1, 100001, 100.001, 'text_0001', '2020-01-01'),
        (2, 100002, 100.002, 'text_0002', '2020-01-02')
```

## ClickHouse reader

Example task configuration to read the above ClickHouse table:

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
