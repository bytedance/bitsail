# FTP/SFTP connector examples

Parent document: [ftp-connector](./ftp.md)

The following configuration shows how to organize parameter configuration to read the following csv format file.

- Example csv data

```csv
c1,c2
aaa,bbb
```

- Configuration file used to read the above file:

```json
{
  "job": {
    "reader": {
      "class": "com.bytedance.bitsail.connector.legacy.ftp.source.FtpInputFormat",
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
