/*
 * Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

ALTER SESSION SET nls_date_language='american';

create table ORACLE_TABLE_SOURCE
(
    ID          VARCHAR2(1024),
    INT_TYPE    NUMBER,
    BIGINT_TYPE NUMBER,
    FLOAT_TYPE  NUMBER,
    DOUBLE_TYPE NUMBER,
    RAW_TYPE    RAW(1024),
    DATE_TYPE   DATE
);

insert into ORACLE_TABLE_SOURCE (ID, INT_TYPE, BIGINT_TYPE, FLOAT_TYPE, DOUBLE_TYPE, RAW_TYPE, DATE_TYPE)
values ('id', 123, 1e12, 1.23, 1.2345, '1afe28', '05-JUL-2022');

create table ORACLE_TABLE_SINK
(
    ID          VARCHAR2(1024),
    DATETIME    NUMBER,
    INT_TYPE    NUMBER,
    BIGINT_TYPE NUMBER,
    FLOAT_TYPE  NUMBER,
    DOUBLE_TYPE NUMBER,
    RAW_TYPE    RAW(1024),
    DATE_TYPE   DATE
);