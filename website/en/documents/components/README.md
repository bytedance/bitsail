---
dir:
  order: 4
---


# Components

-----

## Contents

This module contains components supporting various basic functions, which can be used for developing.
Currently, we support 5 functional components:

- `bitsail-component-clients`:
    - Support conveniently creating some clients, such as KafkaProducer.
    - Details can be seen: [bitsail_component_clients](clients/introduction.md)



- `bitsail-component-formats-flink`:
    - Support converting different data types (such as hive `Writables`) into `bitsail rows`.
    - Details can be seen: [bitsail_component_format](format/introduction.md)


- `bitsail-conversion-flink`:
    - Support converting `bitsail` rows into different data types (such as hive `Writables`).
    - Details can be seen: [bitsail_component_conversion](conversion/introduction.md)


- `bitsail-flink-row-parser`:
    - Support converting `bytes` array into `bitsail` rows according to determined format.
    - Details can be seen: [bitsail_component_parser](parser/introduction.md)

-----

## How to use

Developers can use these modules by importing them in maven dependencies.

```xml
<dependency>
    <groupId>com.bytedance.bitsail</groupId>
    <artifactId>bitsail-xxxxx</artifactId>
    <version>${revision}</version>
    <scope>compile</scope>
</dependency>
```
