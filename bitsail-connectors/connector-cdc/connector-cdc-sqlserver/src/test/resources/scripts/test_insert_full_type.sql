/*
 * Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
CREATE DATABASE bitsail_test;

USE bitsail_test;
EXEC sys.sp_cdc_enable_db;

CREATE TABLE ExampleTable (
                              ID INTEGER IDENTITY(101,1) NOT NULL PRIMARY KEY,
                              BitColumn bit,
                              TinyIntColumn tinyint,
                              SmallIntColumn smallint,
                              IntColumn int,
                              BigIntColumn bigint,
                              DecimalColumn decimal(10,2),
                              NumericColumn numeric(10,2),
                              SmallMoneyColumn smallmoney,
                              MoneyColumn money,
                              FloatColumn float(24),
                              RealColumn real,
                              DateColumn date,
                              TimeColumn time(7),
                              DateTimeColumn datetime,
                              DateTime2Column datetime2(7),
                              DateTimeOffsetColumn datetimeoffset(7),
                              CharColumn char(10),
                              VarcharColumn varchar(50),
                              TextColumn text,
                              NCharColumn nchar(10),
                              NVarcharColumn nvarchar(50),
                              NTextColumn ntext,
                              BinaryColumn binary(10),
                              VarbinaryColumn varbinary(50),
                              ImageColumn image,
                              XMLColumn xml
);

INSERT INTO ExampleTable (BitColumn, TinyIntColumn, SmallIntColumn, IntColumn, BigIntColumn, DecimalColumn, NumericColumn, SmallMoneyColumn, MoneyColumn, FloatColumn, RealColumn, DateColumn, TimeColumn, DateTimeColumn, DateTime2Column, DateTimeOffsetColumn, CharColumn, VarcharColumn, TextColumn, NCharColumn, NVarcharColumn, NTextColumn, BinaryColumn, VarbinaryColumn, ImageColumn, XMLColumn)
VALUES (1, 255, 32767, 2147483647, 9223372036854775807, 1234.56, 1234.56, 12.34, 1234.56, 1234.56, 1234.56, '2023-05-19', '12:34:56.789', '2023-05-19 12:34:56', '2023-05-19 12:34:56.789', '2023-05-19 12:34:56.789-07:00', 'ABCDEF', 'This is a varchar', 'This is a text', N'你好', N'This is a nvarchar', N'This is a ntext', 0x1234567890, 0x1234567890, 0x1234567890, '<a>b</a>');

EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'ExampleTable', @role_name = NULL, @supports_net_changes = 0;