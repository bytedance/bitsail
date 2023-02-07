# LarkSheet connector-v1 example

Parent document: [LarkSheet connector v1](./larksheet-v1.md)

## Reader example

The sheets to be read are:

 - [test_sheet_1](https://e4163pj5kq.feishu.cn/sheets/shtcnQmZNlZ9PjZUJKT5oU3Sjjg?sheet=ZbzDHq)
 - [test_sheet_2](https://e4163pj5kq.feishu.cn/sheets/shtcnQmZNlZ9PjZUJKT5oU3Sjjg?sheet=FJhAlN)

Here is the example job configuration:

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
