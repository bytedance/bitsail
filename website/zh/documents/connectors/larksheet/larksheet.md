# LarkSheet(飞书表格) 连接器

上级文档：[连接器](../README.md)

**BitSail** 飞书表格连接器可用于支持读取飞书表格，主要功能如下:

 - 支持批式一次读取多张飞书表格
 - 支持token和 [application](https://open.feishu.cn/document/ukTMukTMukTM/uYTM5UjL2ETO14iNxkTN/terminology?lang=en-US) 两种鉴权方式
 - 支持读取表格中的部分列


## 依赖引入

```xml
<dependency>
   <groupId>com.bytedance.bitsail</groupId>
   <artifactId>bitsail-connector-larksheet</artifactId>
   <version>${revision}</version>
</dependency>
```

### 飞书表格读取

### 支持数据类型

飞书表格连接器以 `string` 格式读取所有数据。

### 参数

读连接器参数在`job.reader`中配置，实际使用时请注意路径前缀。示例:

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

#### 必需参数

| 参数名称       | 是否必须 | 参数枚举值 | 参数描述                                                                                      |
|:-----------|:-----|:------|:------------------------------------------------------------------------------------------|
| class      | 是    |       | 飞书表格读连接器名, `com.bytedance.bitsail.connector.legacy.larksheet.source.LarkSheetInputFormat` |
| sheet_urls | 是    |       | 要读取的飞书表格列表。多个表格链接用英文逗号分隔。                                                                 | 
| columns    | 是    |       | 描述字段名称和字段类型。字段名称与飞书表格中的header相关（header即为第一行）。                                             |

下面的参数用于鉴权，用户至少需要设置 (`sheet_token`) 或者 (`app_id` and `app_secret`)其中一种。                                        |

<table>
    <tr>
        <th>参数名称</th>
        <th>是否必须</th>
        <th>参数枚举值</th>
        <th>参数描述</th>
    </tr>
    <tr>
        <td>sheet_token</td>
        <td rowspan="3">至少设置下述一项:<br/>1. sheet_token<br/> 2. app_id 和 app_secret</td>
        <td></td>
        <td>用于<a href="https://open.feishu.cn/document/ukTMukTMukTM/ugTMzUjL4EzM14COxMTN">飞书 open api</a>鉴权的token.</td>
    </tr>
    <tr>
        <td>app_id</td>
        <td></td>
        <td rowspan="2">使用 app_id 和 app_secret 来生成用于<a href="https://open.feishu.cn/document/ukTMukTMukTM/ugTMzUjL4EzM14COxMTN">飞书 open api</a>鉴权的token.</td>
    </tr>
    <tr>
        <td>app_secret</td>
        <td></td>
    </tr>
</table>

注意，`sheet_token`可能在任务运行中过期。
如果使用`app_id` 和 `app_secret`，会主动刷新过期token。


#### 可选参数

| 参数名称                   | 是否必须 | 参数枚举值 | 参数描述                     |
|:-----------------------|:-----|:------|:-------------------------|
| reader_parallelism_num | 否    |       | 读并发                      |
| batch_size             | 否    |       | 从open api一次拉取的数据行数       |
| skip_nums              | 否    |       | 对于每个表格可指定跳过开头的行数。用list表示 |

## 相关文档

配置示例文档：[LarkSheet(飞书表格)连接器示例](./larksheet-example.md)
