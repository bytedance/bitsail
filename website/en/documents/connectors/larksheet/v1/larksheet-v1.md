# LarkSheet connector-v1

Parent document: [Connectors](../../README.md)

The **BitSail** LarkSheet connector supports reading lark sheets.
The main function points are as follows:

 - Support batch read from single or multiple lark sheets at once.
 - Support authentication by static token and [application](https://open.feishu.cn/document/ukTMukTMukTM/uYTM5UjL2ETO14iNxkTN/terminology?lang=en-US).
 - Support read a portion of columns from sheets.


## Maven dependency

```xml
<dependency>
   <groupId>com.bytedance.bitsail</groupId>
   <artifactId>connector-larksheet</artifactId>
   <version>${revision}</version>
</dependency>
```

### LarkSheet reader

### Supported data types

BitSail LarkSheet reader processes all data as string.

### Parameters

The following mentioned parameters should be added to `job.reader` block when using, for example:

```json
{
  "job": {
    "reader": {
      "class": "com.bytedance.bitsail.connector.legacy.larksheet.source.LarkSheetInputFormat",
      "sheet_urls": "https://e4163pj5kq.feishu.cn/sheets/shtcnQmZNlZ9PjZUJKT5oU3Sjjg?sheet=ZbzDHq",
      "columns": [
        {
          "name": "id",
          "type": "string"
        },
        {
          "name": "datetime",
          "type": "string"
        }
      ]
    }
  }
}
```

#### Necessary parameters

| Param name | Required | Optional value | Description                                                                                                 |
|:-----------|:---------|:---------------|:------------------------------------------------------------------------------------------------------------|
| class      | Yes      |                | LarkSheet reader class name, `com.bytedance.bitsail.connector.legacy.larksheet.source.LarkSheetInputFormat` |
| sheet_urls | Yes      |                | A list of sheet to read. Multi sheets urls are separated by comma.                                          | 
| columns    | Yes      |                | Describing fields' names and types.                                                                         |

The following parameters are for authentication, you have to set (`sheet_token`) or (`app_id` and `app_secret`) in your configuration.

<table>
    <tr>
        <th>Param name</th>
        <th>Required</th>
        <th>Optional value</th>
        <th>Description</th>
    </tr>
    <tr>
        <td>sheet_token</td>
        <td rowspan="3">At least set one:<br/>1. sheet_token<br/>2. app_id and app_secret</td>
        <td></td>
        <td>Token for get permission to visit <a href="https://open.feishu.cn/document/ukTMukTMukTM/ugTMzUjL4EzM14COxMTN">feishu open api</a>.</td>
    </tr>
    <tr>
        <td>app_id</td>
        <td></td>
        <td rowspan="2">Use app_id and app_secret to generate token for visiting <a href="https://open.feishu.cn/document/ukTMukTMukTM/ugTMzUjL4EzM14COxMTN">feishu open api</a>.</td>
    </tr>
    <tr>
        <td>app_secret</td>
        <td></td>
    </tr>
</table>

Note that if you use `sheet_token`, it may expire when the job runs.
If you use `app_id` and `app_secret`, the token will be refreshed if it expires.

#### Optional parameters

| Param name             | Required | Optional value | Description                                                                  |
|:-----------------------|:---------|:---------------|:-----------------------------------------------------------------------------|
| reader_parallelism_num | No       |                | Read parallelism num                                                         |
| batch_size             | No       |                | Number of lines extracted once.                                              |
| skip_nums              | no       |                | A list of numbers indicating how many lines should be skipped in each sheet. |

## Related documents

Configuration examples: [LarkSheet connector example](./larksheet-example.md)
