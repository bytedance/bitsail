# FTP/SFTP-v1 连接器示例

上级文档：[FTP/SFTP-v1 连接器](./ftp-v1.md)

下面展示了如何使用用户参数配置读取如下 CSV 格式文件。

- 示例 CSV 数据

```csv
c1,c2
aaa,bbb
```

- 用于读取上述格式文件的配置

```json
{
  "job": {
    "reader": {
      "class": "com.bytedance.bitsail.connector.ftp.source.FtpSource",
      "protocol":"FTP",
      "host": "localhost",
      "port": 21,
      "user": "user",
      "password": "password",
      "path_list": "/upload/",
      "success_file_path": "/upload/_SUCCESS",
      "content_type": "csv",
      "columns": [
        {
          "name": "c1",
          "type": "string"
        },
        {
          "name": "c2",
          "type": "string"
        }
      ]
    }
  }
}
```
