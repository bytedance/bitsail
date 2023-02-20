# LocalFileSystem connector example

Parent document: [LocalFileSystem connector](./localfilesystem.md)

## LocalFileSystem Reader

If the file contains (id, date, localdatetime_value, last_name, bool_value) these five fields, we can use the following configuration to read.

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