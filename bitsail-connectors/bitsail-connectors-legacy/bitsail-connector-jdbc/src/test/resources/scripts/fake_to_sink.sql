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