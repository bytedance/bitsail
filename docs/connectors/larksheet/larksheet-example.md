# LarkSheet connector examples

-----

Parent document: [larksheet-connector](./larksheet.md)

## Reader example

The sheets to be read are:

 - [test_sheet_1](https://bytedance.feishu.cn/sheets/shtcncTJGUhZHj3GOX94gLMKxlh?sheet=lAlPxZ)
 - [test_sheet_2](https://bytedance.feishu.cn/sheets/shtcncTJGUhZHj3GOX94gLMKxlh?sheet=aNSgYr)

Here is the example job configuration:

```json
{
  "job": {
    "reader": {
      "class": "com.bytedance.bitsail.connector.legacy.larksheet.source.LarkSheetInputFormat",
      "sheet_urls": "https://bytedance.feishu.cn/sheets/shtcncTJGUhZHj3GOX94gLMKxlh?sheet=lAlPxZ,https://bytedance.feishu.cn/sheets/shtcncTJGUhZHj3GOX94gLMKxlh?sheet=aNSgYr",
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
