# LarkSheet(飞书表格) 连接器示例

父目录：[LarkSheet(飞书表格)连接器](./larksheet.md)

## 飞书表格读连接器

要读的表格如下

- [test_sheet_1](https://e4163pj5kq.feishu.cn/sheets/shtcnQmZNlZ9PjZUJKT5oU3Sjjg?sheet=ZbzDHq)
- [test_sheet_2](https://e4163pj5kq.feishu.cn/sheets/shtcnQmZNlZ9PjZUJKT5oU3Sjjg?sheet=FJhAlN)

示例任务配置如下:

```json
{
  "job": {
    "reader": {
      "class": "com.bytedance.bitsail.connector.legacy.larksheet.source.LarkSheetInputFormat",
      "sheet_urls": "https://e4163pj5kq.feishu.cn/sheets/shtcnQmZNlZ9PjZUJKT5oU3Sjjg?sheet=ZbzDHq,https://e4163pj5kq.feishu.cn/sheets/shtcnQmZNlZ9PjZUJKT5oU3Sjjg?sheet=FJhAlN",
      "app_id": "fake_app_id",
      "app_secret": "fake_app_secret",
      "batch_size": 1000,
      "skip_nums": [100, 200],
      "columns": [
        {
          "name": "id",
          "type": "string"
        },
        {
          "name": "year",
          "type": "string"
        },
        {
          "name": "month",
          "type": "string"
        },
        {
          "name": "day",
          "type": "string"
        },
        {
          "name": "日期",
          "type": "string"
        }
      ]
    }
  }
}
```
