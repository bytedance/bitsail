# Fake连接器配置示例

父目录: [fake-connector](./fake.md)

-----



## Fake读连接器 

假设你想要生成一个300条的数据集，并且指定每个record有2个字段分别为name、age，则可用如下配置读取。

```json
{
  "job": {
    "reader": {
      "class": "com.bytedance.bitsail.connector.legacy.fake.source.FakeSource",
      "total_count": 300,
      "rate": 100,
      "random_null_rate": 0.1,
      "columns": [
        {
          "name": "name",
          "type": "string"
        },
        {
          "name": "age",
          "type": "int"
        }
      ]
    }
  }
}
```
