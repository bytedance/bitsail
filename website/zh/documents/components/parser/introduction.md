# bitsail-flink-row-parser

----- 

上级文档: [bitsail-components](../README.md)

## 内容

开发者在处理数据时，经常需要处理并解析bytes数据。本模块提供了数种格式的parser用于解析bytes数据。

| 类名                | 支持的格式    | 链接                     |
|-------------------|----------|------------------------|
| `CsvBytesParser`  | CSV      | [link](#jump_csv)      |
| `JsonBytesParser` | JSON     | [link](#jump_json)     |
| `PbBytesParser`   | Protobuf | [link](#jump_protobuf) |


### <span id="jump_csv">CsvBytesParser</span>

`CsvBytesParser`使用`org.apache.commons.csvCSVFormat`来解析csv格式的字符串，并支持以下参数:

 - `job.common.csv_delimiter`: 可通过此参数来配置分隔符，默认为 `,`。
 - `job.common.csv_escape`: 可通过此参数来配置escape字符，默认不设置。
 - `job.common.csv_quote`: 可通过此参数来配置quote字符，默认不设置。
- `job.common.csv_with_null_string`: 可通过此参数来配置null数据的转化值，默认不转化。

#### 示例代码

```
public static void main(String[] args) throws Exception {
    String line = "123,test_string,3.14";
    RowTypeInfo rowTypeInfo = new RowTypeInfo(
      PrimitiveColumnTypeInfo.LONG_COLUMN_TYPE_INFO,
      PrimitiveColumnTypeInfo.STRING_COLUMN_TYPE_INFO,
      PrimitiveColumnTypeInfo.DOUBLE_COLUMN_TYPE_INFO
    );

    BitSailConfiguration jobConf = BitSailConfiguration.newDefault();
    jobConf.set(RowParserOptions.CSV_DELIMITER, ",");
    jobConf.set(RowParserOptions.CSV_QUOTE, '"');
    jobConf.set(RowParserOptions.CSV_WITH_NULL_STRING, "null");

    CsvBytesParser parser = new CsvBytesParser(jobConf);

    Row row = new Row(3);
    byte[] bytes = line.getBytes();
    parser.parse(row, bytes, 0, bytes.length, "UTF-8", rowTypeInfo);
    System.out.println(row);
}
```

### <span id="jump_json">JsonBytesParser</span>

`JsonBytesParser`使用`com.bytedance.bitsail.common.util.FastJsonUtil`来解析json格式的字符串，并支持以下参数:

 - `job.common.case_insensitive`: 可通过此参数来配置是否对key大小写敏感，默认为`true`。
 - `job.common.json_serializer_features`: 可通过此参数来设置用于`FastJsonUtil`解析时的properties，格式为 `','` 分隔的字符串，例如: `"QuoteFieldNames,WriteNullListAsEmpty"`。
 - `job.common.convert_error_column_as_null`: 可通过此参数来配置是否在字段转化报错时，将该字段设置为null，默认为`false`。

#### 示例代码

```
public static void main(String[] args) {
    String line = "{\"id\":123, \"state\":\"California\", \"county\":\"Los Angeles\"}";
    RowTypeInfo rowTypeInfo = new RowTypeInfo(
      PrimitiveColumnTypeInfo.LONG_COLUMN_TYPE_INFO,
      PrimitiveColumnTypeInfo.STRING_COLUMN_TYPE_INFO,
      PrimitiveColumnTypeInfo.STRING_COLUMN_TYPE_INFO
    );

    BitSailConfiguration jobConf = BitSailConfiguration.newDefault();
    jobConf.set(RowParserOptions.JSON_SERIALIZER_FEATURES, "QuoteFieldNames");
    JsonBytesParser parser = new JsonBytesParser(jobConf);
    
    Row row = new Row(3);
    byte[] bytes = line.getBytes();
    parser.parse(row, bytes, 0, bytes.length, "UTF-8", rowTypeInfo);
    System.out.println(row);
  }
```

### <span id="jump_protobuf">PbBytesParser</span>

`PbBytesParser`使用用户传入的protobuf描述文件来解析bytes数据，支持以下参数:
 
 - `job.common.proto.descriptor`: 此参数为必需参数，用base64方式存储protobuf descriptor。
 - `job.common.proto.class_name`: 此参数为必需参数，指定protobuf描述文件中用于解析的类名。

#### 示例代码 

示例proto文件`test.proto`如下:
```protobuf
syntax = "proto2";

message ProtoTest {
  required string stringRow = 1;
  required float floatRow = 2;
  required int64 int64Row = 3;
}
```

使用上面proto的`PbBytesParser`示例如下:

```
private transient Descriptor descriptor = null;

public void parsePbData(byte[] pbData) throws Exception {
  byte[] descriptor = IOUtils.toByteArray(new File("test.proto").toURI());
  RowTypeInfo rowTypeInfo = new RowTypeInfo(
    PrimitiveColumnTypeInfo.STRING_COLUMN_TYPE_INFO,
    PrimitiveColumnTypeInfo.DOUBLE_COLUMN_TYPE_INFO,
    PrimitiveColumnTypeInfo.LONG_COLUMN_TYPE_INFO
  );
    
  BitSailConfiguration jobConf = BitSailConfiguration.newDefault();
  jobConf.set(RowParserOptions.PROTO_DESCRIPTOR, new String(descriptor));
  jobConf.set(RowParserOptions.PROTO_CLASS_NAME, "ProtoTest");
  PbBytesParser parser = new PbBytesParser(jobConf);

  Row row = new Row(3);
  parser.parse(row, pbData, 0, pbData.length, null, rowTypeInfo);
}
```



