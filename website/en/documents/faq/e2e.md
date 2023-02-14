# End-to-End Test

BitSail supports End-to-End (E2E) test for most connectors.

## 0. Prerequisite

E2E framework use docker to construct test data resources and test executors, so you need to install docker at first.

## 1. What connectors and data sources are supported?

Curent E2E test framework is aim at connector V2, _e.g._, `connector-redis`, `connector-rocketmq`.

Connectors under `bitsail-connectors-legacy` cannot be test.

- Supported test cases: `bitsail-test/bitsail-test-end-to-end/bitsail-test-e2e-connector-v1`


## 2. How to run E2E test

### 1. Run in Local IDE
You can directly run E2E test in local IDE. You may need to run `build.sh` beforehand.

### 2. Run single case in console
You can use `test-e2e.sh` script to test specific case, for example:

```bash
bash test-e2e.sh bitsail-test-e2e-connector-v1-clickhouse
```


### 3. Run all cases

```bash
mvn clean verify -DskipUT=true -DskipITCase=true -DskipE2E=false -D"checkstyle.skip"=true -D"license.skipAddThirdParty"=true --no-snapshot-updates -am -P _maven.oracle.com_
```


## 3. How to prepare a test case

### 1. Prepare data source

There are some existing data sources are supported in `bitsail-test/bitsail-test-end-to-end/bitsail-test-e2e-connector-v1`.

If you cannot find target data source, then you need to implement it at first. 


### 2. Prepare test script

Use `connector-redis` as example, you can use the following script:

 ```json
{
    "job": {
        "common": {
        "job_id": -2413,
        "job_name": "bitsail_fake_to_redis_e2e_test",
        "instance_id": -20413,
        "user_name": "user"
    },
    "reader": {
        "class": "com.bytedance.bitsail.connector.fake.source.FakeSource",
        "total_count": 300,
        "rate": 100,
        "null_percentage": 0,
        "unique_fields": "fake_key",
        "columns": [
            {
                "name": "fake_key",
                "type": "string",
                "properties": "unique"
            },
            {
                "name": "fake_value",
                "type": "string"
            }
        ]
        },
    "writer": {
        "class": "com.bytedance.bitsail.connector.redis.sink.RedisSink",
        "redis_data_type": "string",
        "columns": [
            {
                "name": "fake_key",
                "type": "string"
            },
            {
                "name": "fake_value",
                "type": "string"
            }
        ]
        }
    }
}

```

### 3. Prepare test class

After data sources and scripts being prepared, you only need to read the scripts and submit to test.

Take `fake-to-redis` case as an example:

- `AbstractE2ETest`: Abstract E2E test class. Users can extend this class to submit script to E2E test executor.

```java
public class FakeToRedisE2ETest extends AbstractE2ETest {

  @Test
  public void testFakeToRedis() throws Exception {
    BitSailConfiguration jobConf = BitSailConfiguration.from(
        new File(Paths.get(getClass().getClassLoader()
            .getResource("fake_to_redis.json")
            .toURI()).toString()));
    jobConf.set(FakeReaderOptions.TOTAL_COUNT, 500);

    // Check if there are 500 keys in redis.
    submitFlink11Job(jobConf,
        "test_fake_to_redis",
        dataSource -> Assert.assertEquals(
            500,
            ((RedisDataSource) dataSource).getKeyCount()
        ));
  }
}
```


