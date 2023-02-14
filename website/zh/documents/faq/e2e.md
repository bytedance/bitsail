# End-to-End 测试

BitSail为大部分connector添加了End-to-End(E2E)测试，下面详细介绍下如何运行测试和自己建立一个测试。

## 0. 前置条件

E2E框架使用Docker构建测试数据源 & 测试容器，所以需要先在本地安装docker。

## 1. 当前支持测试哪些connector和数据源。

目前的E2E框架针对 V1 版本的connector进行设计，比如 `connector-redis`, `connector-rocketmq`。
但对于`bitsail-connectors-legacy`下的connector不支持测试。

 - 支持的测试样例统一在 `bitsail-test/bitsail-test-end-to-end/bitsail-test-e2e-connector-v1` 模块中进行管理。


## 2. 如何进行测试

### 1. 本地IDE运行
如果运行中发现BitSail lib包文件缺失，可以先在本地执行build.sh脚本，再进行测试。

### 2. 命令行运行单个测试样例
可以通过脚本 `test-e2e.sh` 运行指定测试样例，例如:

`bash test-e2e.sh bitsail-test-e2e-connector-v1-clickhouse`
    
### 3. 命令行运行所有测试样例 
通过maven命令进行测试: 

```bash
mvn clean verify -DskipUT=true -DskipITCase=true -DskipE2E=false -D"checkstyle.skip"=true -D"license.skipAddThirdParty"=true --no-snapshot-updates -am -P _maven.oracle.com_
```
    

## 3. 如何编写一个测试样例

### 1. 准备测试数据源

目前已有一些测试数据源在 `bitsail-test/bitsail-test-end-to-end/bitsail-test-e2e-connector-v1` 模块中进行了支持。

如果用户需要测试的conenctor所关联的数据源并未实现，则需要先实现一个测试数据源。
    
### 2. 准备测试脚本

以redis为例，可以准备如下脚本:
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

### 3. 准备测试类

数据源和测试脚本准备好后，只需要读取脚本，并提交测试即可。以fake-to-redis任务为例:

 - `AbstractE2ETest`: E2E抽象类，用户可通过继承此类来使用submitFlink11Job等方法提交E2E测试作业。

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


