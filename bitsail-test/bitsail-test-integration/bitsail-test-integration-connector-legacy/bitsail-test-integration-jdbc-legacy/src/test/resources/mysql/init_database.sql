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

use test;

CREATE TABLE `jdbc_dynamic_table`
(
    `id`             bigint unsigned NOT NULL AUTO_INCREMENT,
    `tinyint_type`   int                                      NOT NULL DEFAULT '0',
    `smallint_type`  int                                      NOT NULL DEFAULT '0',
    `mediumint_type` int                                      NOT NULL DEFAULT '0',
    `int_type`       int                                      NOT NULL DEFAULT '0',
    `bigint_type`    bigint                                            DEFAULT '0',
    `timestamp_type` varchar(128) COLLATE utf8mb4_general_ci           DEFAULT NULL,
    `datetime_type`  varchar(128) COLLATE utf8mb4_general_ci           DEFAULT NULL,
    `float_type`     decimal(10, 4)                                    DEFAULT NULL,
    `double_type`    decimal(20, 4)                                    DEFAULT NULL,
    `date_type`      varchar(128) COLLATE utf8mb4_general_ci           DEFAULT NULL,
    `time_type`      varchar(128) COLLATE utf8mb4_general_ci           DEFAULT NULL,
    `year_type`      varchar(128) COLLATE utf8mb4_general_ci           DEFAULT NULL,
    `char_type`      varchar(10) COLLATE utf8mb4_general_ci   NOT NULL DEFAULT '',
    `varchar_type`   varchar(1024) COLLATE utf8mb4_general_ci          DEFAULT '',
    `text_type`      varchar(1024) COLLATE utf8mb4_general_ci          DEFAULT '',
    `longtext_type`  varchar(1024) COLLATE utf8mb4_general_ci NOT NULL DEFAULT '',
    `blob_type`      varchar(1024) COLLATE utf8mb4_general_ci NOT NULL DEFAULT '',
    `binary_type`    varchar(10) COLLATE utf8mb4_general_ci   NOT NULL DEFAULT '',
    `datetime`       date                                     NOT NULL COMMENT 'date',
    `tinyint_test`   tinyint unsigned DEFAULT NULL,
    PRIMARY KEY (`id`, `datetime`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

create TABLE `jdbc_source_test`
(
    `id`             bigint unsigned NOT NULL AUTO_INCREMENT,
    `int_type`       int                                      NOT NULL DEFAULT '0',
    `double_type`    decimal(20, 4)                                    DEFAULT NULL,
    `date_type`      varchar(128) COLLATE utf8mb4_general_ci           DEFAULT NULL,
    `varchar_type`   varchar(1024) COLLATE utf8mb4_general_ci          DEFAULT '',
    `datetime`       date                                     NOT NULL COMMENT 'date',
    PRIMARY KEY (`id`, `datetime`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

insert into `jdbc_source_test` (`int_type`, `double_type`, `date_type`, `varchar_type`, `datetime`)
VALUES
(1001, 11.11, '2022-10-01', 'varchar_01', '2022-11-01'),
(1002, 22.22, '2022-10-02', 'varchar_02', '2022-11-02'),
(1003, 33.33, '2022-10-03', 'varchar_03', '2022-11-03'),
(1004, 44.44, '2022-10-04', 'varchar_04', '2022-11-04'),
(1005, 55.55, '2022-10-05', 'varchar_05', '2022-11-05'),
(1006, 66.66, '2022-10-06', 'varchar_06', '2022-11-06'),
(1007, 77.77, '2022-10-07', 'varchar_07', '2022-11-07'),
(1008, 88.88, '2022-10-08', 'varchar_08', '2022-11-08'),
(1009, 99.99, '2022-10-09', 'varchar_09', '2022-11-09'),
(1010, 10.10, '2022-10-10', 'varchar_10', '2022-11-10');
