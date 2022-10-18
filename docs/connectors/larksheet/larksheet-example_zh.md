# LarkSheet connector examples

-----

父目录: [飞书表格连接器](./larksheet_zh.md)

## 读连接器示例

要读的表格如下

- [test_sheet_1](https://bytedance.feishu.cn/sheets/shtcncTJGUhZHj3GOX94gLMKxlh?sheet=lAlPxZ)
- [test_sheet_2](https://bytedance.feishu.cn/sheets/shtcncTJGUhZHj3GOX94gLMKxlh?sheet=aNSgYr)

示例任务配置如下:

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
