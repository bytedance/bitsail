# FTP/SFTP-v1 connector

Parent document: [Connectors](../../README.md)

## Main functionalities

This connector can be used to read files from FTP/SFTP servers in batch scenarios. Its functionalities mainly include:

- Support reading files in multiple directories
- Support reading files of various formats

## Maven dependency

```xml
<dependency>
   <groupId>com.bytedance.bitsail</groupId>
   <artifactId>connector-ftp</artifactId>
   <version>${revision}</version>
</dependency>
```

## Supported data types

- Basic data types supported:
    - Integer type:
        - tinyint
        - smallint
        - int
        - bigint
    - Float type:
        - float
        - double
        - decimal
    - Time type:
        - timestamp
        - date
    - String type:
        - string
        - varchar
        - char
    - Bool type:
        - boolean
    - Binary type:
        - binary
- Composited data types supported:
    - map
    - array

## Parameters

The following mentioned parameters should be added to `job.reader` block when using, for example: [ftp-connector-example](./ftp-v1-example.md)

### Necessary parameters

| Param name   | Required | Optional value   | Description                                                                                   |
| :----------- | :------- | :--------------- | :-------------------------------------------------------------------------------------------- |
| class        | Yes      |                  | Class name of connector,`com.bytedance.bitsail.connector.ftp.source.FtpSource`             |
| path_list    | Yes      |                  | Specifies the path of the read in file. Multiple paths can be specified, separated by `','` |
| content_type | Yes      | JSON/CSV | Specify the format of the read in file. For details, refer to  [Supported formats](#jump_format)      |
| columns      | Yes      |                  | Describing fields' names and types                                                            |
| port | Yes |  | Server port，normally FTP is 21, SFTP is 22 |
| host | Yes |  | Server host |
| user | Yes |  | Username |
| password | Yes |  | Password |
| protocol | Yes | FTP/SFTP | Protocol |
| success_file_path | Yes |  | Path to SUCCESS tag file |

### Optional parameters

| Param name             | Required | Default value | Optional value | Description                                                  |
| :--------------------- | :------- | :------ | ---- | :----------------------------------------------------------- |
| connect_pattern           | No          | PASV if FTP, NULL if SFTP | PASV/PORT/NULL     | In ftp mode, connect pattern can be PASV or PORT. In sftp mode, connect pattern is NULL |
| time_out                  | No         | 5000ms               |                | Connection timeout                       |
| enable_success_file_check | No         | True                 |                | Enabled by default, the job will not start if SUCCESS tag doesn't exist |
| max_retry_time            | No         | 60                   |                | Max time to check for SUCCESS tag file |
| retry_interval_ms         | No         | 60s                  |                | Retry interval to check for SUCCESS tag file |

## <span id="jump_format">Supported formats</span>

Support the following formats(configured by `content_type`):

- [JSON](#jump_json)
- [CSV](#jump_csv)


### <span id="jump_json">JSON</span>

It supports parsing text files in json format. Each line is required to be a standard json string.

The following parameters are supported to adjust the json parsing stype:

| Parameter name                            | Default value | Description                                                      |
| ----------------------------------------- |---------------| -----------------------------------------------------------------|
| `job.reader.case_insensitive`             | false         | Whether to be sensitive to the case of the key in the json field |
| `job.reader.convert_error_column_as_null` | false         | Whether to set the field with parsing error to null              |

### <span id="jump_csv">CSV</span>

Support parsing of text files in csv format. Each line is required to be a standard csv string.

The following parameters are supported to adjust the csv parsing style:

| Parameter name                    | Default value | Description                                                                |
| --------------------------------- | ------------- | -------------------------------------------------------------------------- |
| `job.reader.csv_delimiter`        | `','`         | csv delimiter                                                              |
| `job.reader.csv_escape`           |               | escape character                                                           |
| `job.reader.csv_quote`            |               | quote character                                                            |
| `job.reader.csv_with_null_string` |               | Specify the conversion value of null field. It is not converted by default |

## Related documents

Configuration examples: [FTP/SFTP-v1 connector example](./ftp-v1-example.md)
