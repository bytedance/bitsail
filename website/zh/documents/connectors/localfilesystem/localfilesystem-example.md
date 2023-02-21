# LocalFileSystem 连接器

上级文档： [LocalFileSystem connector](./localfilesystem.md)

## LocalFileSystem 读连接器

如果文件中包含（id, date, localdatetime_value, last_name, bool_value）这五个字段，我们可以使用如下配置来读取。

```json
{
  "job": {
    "reader": {
      "class": "com.bytedance.bitsail.connector.localfilesystem.source.LocalFileSystemSource",
      "file_path": "src/test/resources/data/csv/test.csv",
      "content_type": "csv",
      "columns": [
        {
          "name": "id",
          "type": "long"
        },
        {
          "name": "date",
          "type": "date"
        },
        {
          "name": "localdatetime_value",
          "type": "timestamp"
        },
        {
          "name": "last_name",
          "type": "string"
        },
        {
          "name": "bool_value",
          "type": "boolean"
        }
      ]
    }
  }
}
```