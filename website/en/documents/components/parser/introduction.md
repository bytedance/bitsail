# BitSail-flink-row-parser

----- 

Parent document: [bitsail-components](../README.md)

## Content

When developers processing data, they often need to process and parse bytes data. This module provides parsers for parsing several formats of bytes data.

| Name              | Supported format | link                   |
|-------------------|------------------|------------------------|
| `CsvBytesParser`  | CSV              | [link](#jump_csv)      |
| `JsonBytesParser` | JSON             | [link](#jump_json)     |
| `PbBytesParser`   | Protobuf         | [link](#jump_protobuf) |


### <span id="jump_csv">CsvBytesParser</span>

`CsvBytesParser` uses `org.apache.commons.csvCSVFormat` to parse csv format strings, and supporting the following parameters:

- `job.common.csv_delimiter`: This parameter can be used to configure the delimiter, the default is `','`
- `job.common.csv_escape`: The escape character can be configured through this parameter, it is not set by default.
- `job.common.csv_quote`: The quote character can be configured through this parameter, it is not set by default.
- `job.common.csv_with_null_string`: This parameter can be used to configure the conversion value of null data, which is not converted by default.


#### Example code

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

`JsonBytesParser` use `com.bytedance.bitsail.common.util.FastJsonUtil` to parse strings in json format, and supports the following parameters:

 - `job.common.case_insensitive`: This parameter can be used to configure whether the key is case sensitive. The default value is true.

 - `job.common.json_serializer_features`: This parameter can be used to set properties used for parsing, the format is a `','` separated string, for example: "QuoteFieldNames,WriteNullListAsEmpty".

 - `job.common.convert_error_column_as_null`: This parameter can be used to configure whether to set the field to null when the field fails to convert. The default value is false.


#### Example code

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

`PbBytesParser` use the protobuf description file passed in by the user to parse the bytes data. The following parameters are supported:

- `job.common.proto.descriptor`: This parameter is required and stores the protobuf descriptor in base64.
- `job.common.proto.class_name`: This parameter is required and specifies the class name used for parsing in the protobuf description file.

#### Example code

A sample proto file`test.proto`:
```protobuf
syntax = "proto2";

message ProtoTest {
  required string stringRow = 1;
  required float floatRow = 2;
  required int64 int64Row = 3;
}
```

An example using the above proto is as follows:

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



