import{_ as s}from"./bitsail_model.b409088b.js";import{_ as a,a as l,b as t,c as d,d as r,e as o,f as c}from"./deserialization_schema_diagram.b71ffa52.js";import{_ as u}from"./_plugin-vue_export-helper.cdc0426e.js";import{o as v,c as m,a as e,b as i,d as p,w as b,e as h,r as g}from"./app.c1da3360.js";const f={},S=e("h1",{id:"source-connector-details",tabindex:"-1"},[e("a",{class:"header-anchor",href:"#source-connector-details","aria-hidden":"true"},"#"),i(" Source Connector Details")],-1),y=h('<hr><h2 id="introduction" tabindex="-1"><a class="header-anchor" href="#introduction" aria-hidden="true">#</a> Introduction</h2><p><img src="'+s+'" alt="" loading="lazy"></p><ul><li>Source: The life cycle management component of the data reading component is mainly responsible for interacting with the framework, structuring the job, and not participating in the actual execution of the job.</li><li>SourceSplit: Source data split, the core purpose of the big data processing framework is to split large-scale data into multiple reasonable Splits</li><li>State：Job status snapshot, when the checkpoint is enabled, the current execution status will be saved.</li><li>SplitCoordinator: SplitCoordinator assumes the role of creating and managing Split.</li><li>SourceReader: The component that is actually responsible for data reading will read the data after receiving the Split, and then transmit the data to the next operator.</li></ul><h2 id="source" tabindex="-1"><a class="header-anchor" href="#source" aria-hidden="true">#</a> Source</h2><p>The life cycle management of the data reading component is mainly responsible for the interaction with the framework and the construction of the job, and it does not participate in the actual execution of the job.</p><p>Take RocketMQSource as an example: the Source method needs to implement the Source and ParallelismComputable interfaces.</p><p><img src="'+a+`" alt="" loading="lazy"></p><h3 id="source-interface" tabindex="-1"><a class="header-anchor" href="#source-interface" aria-hidden="true">#</a> Source Interface</h3><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public interface Source&lt;T, SplitT extends SourceSplit, StateT extends Serializable&gt; extends Serializable {

  /**
   * Run in client side for source initialize;
   */
  void configure(ExecutionEnviron execution, BitSailConfiguration readerConfiguration) throws IOException;

  /**
   * Indicate the Source type.
   */
  Boundedness getSourceBoundedness();

  /**
   * Create Source Reader.
   */
  SourceReader&lt;T, SplitT&gt; createReader(SourceReader.Context readerContext);

  /**
   * Create split coordinator.
   */
  SourceSplitCoordinator&lt;SplitT, StateT&gt; createSplitCoordinator(SourceSplitCoordinator.Context&lt;SplitT, StateT&gt; coordinatorContext);

  /**
   * Get Split serializer for the framework,{@link SplitT}should implement from {@link  Serializable}
   */
  default BinarySerializer&lt;SplitT&gt; getSplitSerializer() {
    return new SimpleBinarySerializer&lt;&gt;();
  }

  /**
   * Get State serializer for the framework, {@link StateT}should implement from {@link  Serializable}
   */
  default BinarySerializer&lt;StateT&gt; getSplitCoordinatorCheckpointSerializer() {
    return new SimpleBinarySerializer&lt;&gt;();
  }

  /**
   * Create type info converter for the source, default value {@link BitSailTypeInfoConverter}
   */
  default TypeInfoConverter createTypeInfoConverter() {
    return new BitSailTypeInfoConverter();
  }

  /**
   * Get Source&#39;s name.
   */
  String getReaderName();
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h4 id="configure-method" tabindex="-1"><a class="header-anchor" href="#configure-method" aria-hidden="true">#</a> configure method</h4><p>We mainly do the distribution and extraction of some client configurations, and can operate on the configuration of the runtime environment <code>ExecutionEnviron</code> and <code>readerConfiguration</code>.</p><h5 id="example" tabindex="-1"><a class="header-anchor" href="#example" aria-hidden="true">#</a> example</h5><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>@Override
public void configure(ExecutionEnviron execution, BitSailConfiguration readerConfiguration) {
  this.readerConfiguration = readerConfiguration;
  this.commonConfiguration = execution.getCommonConfiguration();
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h4 id="getsourceboundedness-method" tabindex="-1"><a class="header-anchor" href="#getsourceboundedness-method" aria-hidden="true">#</a> getSourceBoundedness method</h4><p>Set the processing method of the job, which is to use the stream processing method, the batch processing method, or the stream-batch unified processing method. In the stream-batch integrated examples, we need to set different processing methods according to different types of jobs。</p><table><thead><tr><th>Job Type</th><th>Boundedness</th></tr></thead><tbody><tr><td>batch</td><td>Boundedness.<em>BOUNDEDNESS</em></td></tr><tr><td>stream</td><td>Boundedness.<em>UNBOUNDEDNESS</em></td></tr></tbody></table><h5 id="unified-example" tabindex="-1"><a class="header-anchor" href="#unified-example" aria-hidden="true">#</a> Unified example</h5><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>@Override
public Boundedness getSourceBoundedness() {
  return Mode.BATCH.equals(Mode.getJobRunMode(commonConfiguration.get(CommonOptions.JOB_TYPE))) ?
      Boundedness.BOUNDEDNESS :
      Boundedness.UNBOUNDEDNESS;
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h5 id="batch-example" tabindex="-1"><a class="header-anchor" href="#batch-example" aria-hidden="true">#</a> Batch example</h5><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public Boundedness getSourceBoundedness() {
  return Boundedness.BOUNDEDNESS;
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h4 id="createtypeinfoconverter-method" tabindex="-1"><a class="header-anchor" href="#createtypeinfoconverter-method" aria-hidden="true">#</a> createTypeInfoConverter method</h4><p>A type converter used to specify the Source connector; we know that most external data systems have their own type definitions, and their definitions will not be completely consistent with BitSail’s type definitions; in order to simplify the conversion of type definitions, we support the relationship between the two being mapped through the configuration file, thereby simplifying the development of the configuration file.</p><p>It is the parsing of the <code>columns</code> in the <code>reader</code> part of the task description Json file. The type of different fields in the <code>columns</code> will be parsed from the <code>ClickhouseReaderOptions.COLUMNS</code> field to <code>readerContext.getTypeInfos()</code> according to the above description file。</p><h5 id="example-1" tabindex="-1"><a class="header-anchor" href="#example-1" aria-hidden="true">#</a> example</h5><ul><li><code>BitSailTypeInfoConverter</code><ul><li>Default <code>TypeInfoConverter</code>，Directly parse the string of the <code>ReaderOptions.COLUMNS</code> field, what type is in the <code>COLUMNS</code> field, and what type is in <code>TypeInfoConverter</code>.</li></ul></li><li><code>FileMappingTypeInfoConverter</code><ul><li>It will bind the <code>{readername}-type-converter.yaml</code> file during BitSail type system conversion to map the database field type and BitSail type. The <code>ReaderOptions.COLUMNS</code> field will be mapped to <code>TypeInfoConverter</code> after being converted by this mapping file.</li></ul></li></ul><h6 id="filemappingtypeinfoconverter" tabindex="-1"><a class="header-anchor" href="#filemappingtypeinfoconverter" aria-hidden="true">#</a> FileMappingTypeInfoConverter</h6><p>Databases connected through JDBC, including MySql, Oracle, SqlServer, Kudu, ClickHouse, etc. The characteristic of the data source here is to return the obtained data in the form of <code>java.sql.ResultSet</code> interface. For this type of database, we often design the <code>TypeInfoConverter</code> object as <code>FileMappingTypeInfoConverter</code>. This object will be bound to <code>{readername}-type-converter.yaml</code> file during BitSail type system conversion, which is used to map the database field type and BitSail type.</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>@Override
public TypeInfoConverter createTypeInfoConverter() {
  return new FileMappingTypeInfoConverter(getReaderName());
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p>For the parsing of the <code>{readername}-type-converter.yaml</code> file, take <code>clickhouse-type-converter.yaml</code> as an example.</p><div class="language-Plain line-numbers-mode" data-ext="Plain"><pre class="language-Plain"><code># Clickhouse Type to BitSail Type
engine.type.to.bitsail.type.converter:

  - source.type: int32
    target.type: int

  - source.type: float64
    target.type: double

  - source.type: string
    target.type: string

  - source.type: date
    target.type: date.date

  - source.type: null
    target.type: void

# BitSail Type to Clickhouse Type
bitsail.type.to.engine.type.converter:

  - source.type: int
    target.type: int32

  - source.type: double
    target.type: float64

  - source.type: date.date
    target.type: date

  - source.type: string
    target.type: string
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p>The role of this file is to analyze the <code>columns</code> in the <code>reader </code>part of the job description json file. The types of different fields in the <code>columns</code> will be parsed from the <code>ClickhouseReaderOptions.COLUMNS</code> field to <code>readerContext.getTypeInfos()</code> according to the above description file.</p><div class="language-Json line-numbers-mode" data-ext="Json"><pre class="language-Json"><code>&quot;reader&quot;: {
  &quot;class&quot;: &quot;com.bytedance.bitsail.connector.clickhouse.source.ClickhouseSource&quot;,
  &quot;jdbc_url&quot;: &quot;jdbc:clickhouse://localhost:8123&quot;,
  &quot;db_name&quot;: &quot;default&quot;,
  &quot;table_name&quot;: &quot;test_ch_table&quot;,
  &quot;split_field&quot;: &quot;id&quot;,
  &quot;split_config&quot;: &quot;{\\&quot;name\\&quot;: \\&quot;id\\&quot;, \\&quot;lower_bound\\&quot;: 0, \\&quot;upper_bound\\&quot;: \\&quot;10000\\&quot;, \\&quot;split_num\\&quot;: 3}&quot;,
  &quot;sql_filter&quot;: &quot;( id % 2 == 0 )&quot;,
  &quot;columns&quot;: [
    {
      &quot;name&quot;: &quot;id&quot;,
      &quot;type&quot;: &quot;int64&quot;
    },
    {
      &quot;name&quot;: &quot;int_type&quot;,
      &quot;type&quot;: &quot;int32&quot;
    },
    {
      &quot;name&quot;: &quot;double_type&quot;,
      &quot;type&quot;: &quot;float64&quot;
    },
    {
      &quot;name&quot;: &quot;string_type&quot;,
      &quot;type&quot;: &quot;string&quot;
    },
    {
      &quot;name&quot;: &quot;p_date&quot;,
      &quot;type&quot;: &quot;date&quot;
    }
  ]
},
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p><img src="`+l+`" alt="" loading="lazy"></p><p>This method is not only applicable to databases, but also applicable to all scenarios that require type mapping between the engine side and the BitSail side during type conversion.</p><h6 id="bitsailtypeinfoconverter" tabindex="-1"><a class="header-anchor" href="#bitsailtypeinfoconverter" aria-hidden="true">#</a> BitSailTypeInfoConverter</h6><p>Usually, the default method is used for type conversion, and the string is directly parsed for the <code>ReaderOptions.COLUMNS</code>field.</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>@Override
public TypeInfoConverter createTypeInfoConverter() {
  return new BitSailTypeInfoConverter();
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p>BitSailTypeInfoConverter</p><p>Usually, the default method is used for type conversion, and the string is directly parsed for the <code>ReaderOptions.COLUMNS</code> field.</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>@Override
public TypeInfoConverter createTypeInfoConverter() {
  return new BitSailTypeInfoConverter();
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p>Take Hadoop as an example:</p><div class="language-Json line-numbers-mode" data-ext="Json"><pre class="language-Json"><code>&quot;reader&quot;: {
  &quot;class&quot;: &quot;com.bytedance.bitsail.connector.hadoop.source.HadoopSource&quot;,
  &quot;path_list&quot;: &quot;hdfs://127.0.0.1:9000/test_namespace/source/test.json&quot;,
  &quot;content_type&quot;:&quot;json&quot;,
  &quot;reader_parallelism_num&quot;: 1,
  &quot;columns&quot;: [
    {
      &quot;name&quot;:&quot;id&quot;,
      &quot;type&quot;: &quot;int&quot;
    },
    {
      &quot;name&quot;: &quot;string_type&quot;,
      &quot;type&quot;: &quot;string&quot;
    },
    {
      &quot;name&quot;: &quot;map_string_string&quot;,
      &quot;type&quot;: &quot;map&lt;string,string&gt;&quot;
    },
    {
      &quot;name&quot;: &quot;array_string&quot;,
      &quot;type&quot;: &quot;list&lt;string&gt;&quot;
    }
  ]
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p><img src="`+t+`" alt="" loading="lazy"></p><h4 id="createsourcereader-method" tabindex="-1"><a class="header-anchor" href="#createsourcereader-method" aria-hidden="true">#</a> createSourceReader method</h4><p>Write the specific data reading logic. The component responsible for data reading will read the data after receiving the Split, and then transmit the data to the next operator.</p><p>The specific parameters passed to construct <code>SourceReader</code> are determined according to requirements, but it must be ensured that all parameters can be serialized. If it is not serializable, an error will occur when <code>createJobGraph</code> is created.</p><h5 id="example-2" tabindex="-1"><a class="header-anchor" href="#example-2" aria-hidden="true">#</a> example</h5><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public SourceReader&lt;Row, RocketMQSplit&gt; createReader(SourceReader.Context readerContext) {
  return new RocketMQSourceReader(
      readerConfiguration,
      readerContext,
      getSourceBoundedness());
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h4 id="createsplitcoordinator-method" tabindex="-1"><a class="header-anchor" href="#createsplitcoordinator-method" aria-hidden="true">#</a> createSplitCoordinator method</h4><p>Writing specific data split and split allocation logic, the SplitCoordinator assumes the role of creating and managing Splits</p><p>The specific parameters passed to construct <code>SplitCoordinator </code>are determined according to requirements, but it must be ensured that all parameters can be serialized. If it is not serializable, an error will occur when <code>createJobGraph</code> is created.</p><h5 id="example-3" tabindex="-1"><a class="header-anchor" href="#example-3" aria-hidden="true">#</a> example</h5><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public SourceSplitCoordinator&lt;RocketMQSplit, RocketMQState&gt; createSplitCoordinator(SourceSplitCoordinator
                                                                                       .Context&lt;RocketMQSplit, RocketMQState&gt; coordinatorContext) {
  return new RocketMQSourceSplitCoordinator(
      coordinatorContext,
      readerConfiguration,
      getSourceBoundedness());
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="parallelismcomputable-interface" tabindex="-1"><a class="header-anchor" href="#parallelismcomputable-interface" aria-hidden="true">#</a> ParallelismComputable Interface</h3><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public interface ParallelismComputable extends Serializable {

  /**
   * give a parallelism advice for reader/writer based on configurations and upstream parallelism advice
   *
   * @param commonConf     common configuration
   * @param selfConf       reader/writer configuration
   * @param upstreamAdvice parallelism advice from upstream (when an operator has no upstream in DAG, its upstream is
   *                       global parallelism)
   * @return parallelism advice for the reader/writer
   */
  ParallelismAdvice getParallelismAdvice(BitSailConfiguration commonConf,
                                         BitSailConfiguration selfConf,
                                         ParallelismAdvice upstreamAdvice) throws Exception;
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h4 id="getparallelismadvice-method" tabindex="-1"><a class="header-anchor" href="#getparallelismadvice-method" aria-hidden="true">#</a> getParallelismAdvice method</h4><p>Used to specify the parallel number of downstream readers. Generally, there are the following methods:</p><ul><li>Use <code>selfConf.get(ClickhouseReaderOptions.READER_PARALLELISM_NUM)</code> to specify the degree of parallelism.</li><li>Customize your own parallelism division logic.</li></ul><h5 id="example-4" tabindex="-1"><a class="header-anchor" href="#example-4" aria-hidden="true">#</a> example</h5><p>For example, in RocketMQ, we can define that each reader can handle up to 4 queues. <em><code>DEFAULT_ROCKETMQ_PARALLELISM_THRESHOLD </code></em><code>= 4</code></p><p>Obtain the corresponding degree of parallelism through this custom method.</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public ParallelismAdvice getParallelismAdvice(BitSailConfiguration commonConfiguration,
                                                BitSailConfiguration rocketmqConfiguration,
                                                ParallelismAdvice upstreamAdvice) throws Exception {
    String cluster = rocketmqConfiguration.get(RocketMQSourceOptions.CLUSTER);
    String topic = rocketmqConfiguration.get(RocketMQSourceOptions.TOPIC);
    String consumerGroup = rocketmqConfiguration.get(RocketMQSourceOptions.CONSUMER_GROUP);
    DefaultLitePullConsumer consumer = RocketMQUtils.prepareRocketMQConsumer(rocketmqConfiguration, String.format(SOURCE_INSTANCE_NAME_TEMPLATE,
        cluster,
        topic,
        consumerGroup,
        UUID.randomUUID()
    ));
    try {
      consumer.start();
      Collection&lt;MessageQueue&gt; messageQueues = consumer.fetchMessageQueues(topic);
      int adviceParallelism = Math.max(CollectionUtils.size(messageQueues) / DEFAULT_ROCKETMQ_PARALLELISM_THRESHOLD, 1);

      return ParallelismAdvice.builder()
          .adviceParallelism(adviceParallelism)
          .enforceDownStreamChain(true)
          .build();
    } finally {
      consumer.shutdown();
    }
  }
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h2 id="sourcesplit" tabindex="-1"><a class="header-anchor" href="#sourcesplit" aria-hidden="true">#</a> SourceSplit</h2><p>The data fragmentation format of the data source requires us to implement the SourceSplit interface.</p><p><img src="`+d+`" alt="" loading="lazy"></p><h3 id="sourcesplit-interface" tabindex="-1"><a class="header-anchor" href="#sourcesplit-interface" aria-hidden="true">#</a> SourceSplit Interface</h3><p>We are required to implement a method to obtain splitId.</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public interface SourceSplit extends Serializable {
  String uniqSplitId();
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p>For the specific slice format, developers can customize it according to their own needs.</p><h3 id="example-5" tabindex="-1"><a class="header-anchor" href="#example-5" aria-hidden="true">#</a> example</h3><h4 id="database" tabindex="-1"><a class="header-anchor" href="#database" aria-hidden="true">#</a> Database</h4><p>Generally, the primary key is used to divide the data into maximum and minimum values; for classes without a primary key, it is usually recognized as a split and no longer split, so the parameters in the split include the maximum and minimum values of the primary key, and a Boolean type <code>readTable</code>. If there is no primary key class or the primary key is not split, the entire table will be regarded as a split. Under this condition, <code>readTable</code> is true. If the primary key is split according to the maximum and minimum values, it is set to false.。</p><p>Take ClickhouseSourceSplit as an example:</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>@Setter
public class ClickhouseSourceSplit implements SourceSplit {
  public static final String SOURCE_SPLIT_PREFIX = &quot;clickhouse_source_split_&quot;;
  private static final String BETWEEN_CLAUSE = &quot;( \`%s\` BETWEEN ? AND ? )&quot;;

  private final String splitId;

  /**
   * Read whole table or range [lower, upper]
   */
  private boolean readTable;
  private Long lower;
  private Long upper;

  public ClickhouseSourceSplit(int splitId) {
    this.splitId = SOURCE_SPLIT_PREFIX + splitId;
  }

  @Override
  public String uniqSplitId() {
    return splitId;
  }

  public void decorateStatement(PreparedStatement statement) {
    try {
      if (readTable) {
        lower = Long.MIN_VALUE;
        upper = Long.MAX_VALUE;
      }
      statement.setObject(1, lower);
      statement.setObject(2, upper);
    } catch (SQLException e) {
      throw BitSailException.asBitSailException(CommonErrorCode.RUNTIME_ERROR, &quot;Failed to decorate statement with split &quot; + this, e.getCause());
    }
  }

  public static String getRangeClause(String splitField) {
    return StringUtils.isEmpty(splitField) ? null : String.format(BETWEEN_CLAUSE, splitField);
  }

  @Override
  public String toString() {
    return String.format(
        &quot;{\\&quot;split_id\\&quot;:\\&quot;%s\\&quot;, \\&quot;lower\\&quot;:%s, \\&quot;upper\\&quot;:%s, \\&quot;readTable\\&quot;:%s}&quot;,
        splitId, lower, upper, readTable);
  }
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h4 id="message-queue" tabindex="-1"><a class="header-anchor" href="#message-queue" aria-hidden="true">#</a> Message queue</h4><p>Generally, splits are divided according to the number of partitions registered in the topic in the message queue. The slice should mainly include the starting point and end point of consumption and the queue of consumption.</p><p>Take RocketMQSplit as an example:</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>@Builder
@Getter
public class RocketMQSplit implements SourceSplit {

  private MessageQueue messageQueue;

  @Setter
  private long startOffset;

  private long endOffset;

  private String splitId;

  @Override
  public String uniqSplitId() {
    return splitId;
  }

  @Override
  public String toString() {
    return &quot;RocketMQSplit{&quot; +
        &quot;messageQueue=&quot; + messageQueue +
        &quot;, startOffset=&quot; + startOffset +
        &quot;, endOffset=&quot; + endOffset +
        &#39;}&#39;;
  }
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h4 id="file-system" tabindex="-1"><a class="header-anchor" href="#file-system" aria-hidden="true">#</a> File system</h4><p>Generally, files are divided as the smallest granularity, and some formats also support splitting a single file into multiple sub-Splits. The required file slices need to be packed in the file system split.</p><p>Take <code>FtpSourceSplit</code> as an example:</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public class FtpSourceSplit implements SourceSplit {

  public static final String FTP_SOURCE_SPLIT_PREFIX = &quot;ftp_source_split_&quot;;

  private final String splitId;

  @Setter
  private String path;
  @Setter
  private long fileSize;

  public FtpSourceSplit(int splitId) {
    this.splitId = FTP_SOURCE_SPLIT_PREFIX + splitId;
  }

  @Override
  public String uniqSplitId() {
    return splitId;
  }

  @Override
  public boolean equals(Object obj) {
    return (obj instanceof FtpSourceSplit) &amp;&amp; (splitId.equals(((FtpSourceSplit) obj).splitId));
  }

}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p>In particular, in the Hadoop file system, we can also use the wrapper of the <code>org.apache.hadoop.mapred.InputSpli</code>t class to customize our Split.</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public class HadoopSourceSplit implements SourceSplit {
  private static final long serialVersionUID = 1L;
  private final Class&lt;? extends InputSplit&gt; splitType;
  private transient InputSplit hadoopInputSplit;

  private byte[] hadoopInputSplitByteArray;

  public HadoopSourceSplit(InputSplit inputSplit) {
    if (inputSplit == null) {
      throw new NullPointerException(&quot;Hadoop input split must not be null&quot;);
    }

    this.splitType = inputSplit.getClass();
    this.hadoopInputSplit = inputSplit;
  }

  public InputSplit getHadoopInputSplit() {
    return this.hadoopInputSplit;
  }

  public void initInputSplit(JobConf jobConf) {
    if (this.hadoopInputSplit != null) {
      return;
    }

    checkNotNull(hadoopInputSplitByteArray);

    try {
      this.hadoopInputSplit = (InputSplit) WritableFactories.newInstance(splitType);

      if (this.hadoopInputSplit instanceof Configurable) {
        ((Configurable) this.hadoopInputSplit).setConf(jobConf);
      } else if (this.hadoopInputSplit instanceof JobConfigurable) {
        ((JobConfigurable) this.hadoopInputSplit).configure(jobConf);
      }

      if (hadoopInputSplitByteArray != null) {
        try (ObjectInputStream objectInputStream = new ObjectInputStream(new ByteArrayInputStream(hadoopInputSplitByteArray))) {
          this.hadoopInputSplit.readFields(objectInputStream);
        }

        this.hadoopInputSplitByteArray = null;
      }
    } catch (Exception e) {
      throw new RuntimeException(&quot;Unable to instantiate Hadoop InputSplit&quot;, e);
    }
  }

  private void writeObject(ObjectOutputStream out) throws IOException {

    if (hadoopInputSplit != null) {
      try (
          ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
          ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)
      ) {
        this.hadoopInputSplit.write(objectOutputStream);
        objectOutputStream.flush();
        this.hadoopInputSplitByteArray = byteArrayOutputStream.toByteArray();
      }
    }
    out.defaultWriteObject();
  }

  @Override
  public String uniqSplitId() {
    return hadoopInputSplit.toString();
  }
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h2 id="state" tabindex="-1"><a class="header-anchor" href="#state" aria-hidden="true">#</a> State</h2><p>In scenarios where checkpoints are required, we usually use <code>Map</code> to preserve the current execution state.</p><h3 id="unified-example-1" tabindex="-1"><a class="header-anchor" href="#unified-example-1" aria-hidden="true">#</a> Unified example</h3><p>In the streaming-batch unified scenario, we need to save the state to recover from the abnormally interrupted streaming job.</p><p>Take RocketMQState as an example:</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public class RocketMQState implements Serializable {

  private final Map&lt;MessageQueue, String&gt; assignedWithSplitIds;

  public RocketMQState(Map&lt;MessageQueue, String&gt; assignedWithSplitIds) {
    this.assignedWithSplitIds = assignedWithSplitIds;
  }

  public Map&lt;MessageQueue, String&gt; getAssignedWithSplits() {
    return assignedWithSplitIds;
  }
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="batch-example-1" tabindex="-1"><a class="header-anchor" href="#batch-example-1" aria-hidden="true">#</a> Batch example</h3><p>For batch scenarios, we can use EmptyState to not store the state. If state storage is required, a similar design scheme is adopted for the stream-batch unified scenario.</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public class EmptyState implements Serializable {

  public static EmptyState fromBytes() {
    return new EmptyState();
  }
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h2 id="sourcesplitcoordinator" tabindex="-1"><a class="header-anchor" href="#sourcesplitcoordinator" aria-hidden="true">#</a> SourceSplitCoordinator</h2><p>The core purpose of the big data processing framework is to split large-scale data into multiple reasonable Splits, and the SplitCoordinator assumes the role of creating and managing Splits.</p><p><img src="`+r+`" alt="" loading="lazy"></p><h3 id="sourcesplitcoordinator-interface" tabindex="-1"><a class="header-anchor" href="#sourcesplitcoordinator-interface" aria-hidden="true">#</a> SourceSplitCoordinator Interface</h3><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public interface SourceSplitCoordinator&lt;SplitT extends SourceSplit, StateT&gt; extends Serializable, AutoCloseable {

  void start();

  void addReader(int subtaskId);

  void addSplitsBack(List&lt;SplitT&gt; splits, int subtaskId);

  void handleSplitRequest(int subtaskId, @Nullable String requesterHostname);

  default void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
  }

  StateT snapshotState() throws Exception;

  default void notifyCheckpointComplete(long checkpointId) throws Exception {
  }

  void close();

  interface Context&lt;SplitT extends SourceSplit, StateT&gt; {

    boolean isRestored();

    /**
     * Return the state to the split coordinator, for the exactly-once.
     */
    StateT getRestoreState();

    /**
     * Return total parallelism of the source reader.
     */
    int totalParallelism();

    /**
     * When Source reader started, it will be registered itself to coordinator.
     */
    Set&lt;Integer&gt; registeredReaders();

    /**
     * Assign splits to reader.
     */
    void assignSplit(int subtaskId, List&lt;SplitT&gt; splits);

    /**
     * Mainly use in boundedness situation, represents there will no more split will send to source reader.
     */
    void signalNoMoreSplits(int subtask);

    /**
     * If split coordinator have any event want to send source reader, use this method.
     * Like send Pause event to Source Reader in CDC2.0.
     */
    void sendEventToSourceReader(int subtaskId, SourceEvent event);

    /**
     * Schedule to run the callable and handler, often used in un-boundedness mode.
     */
    &lt;T&gt; void runAsync(Callable&lt;T&gt; callable,
                      BiConsumer&lt;T, Throwable&gt; handler,
                      int initialDelay,
                      long interval);

    /**
     * Just run callable and handler once, often used in boundedness mode.
     */
    &lt;T&gt; void runAsyncOnce(Callable&lt;T&gt; callable,
                          BiConsumer&lt;T, Throwable&gt; handler);
  }
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="construction-method" tabindex="-1"><a class="header-anchor" href="#construction-method" aria-hidden="true">#</a> Construction method</h3><p>In the construction method, developers generally mainly perform some configuration settings and create containers for shard information storage.</p><p>Take the construction of ClickhouseSourceSplitCoordinator as an example:</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public ClickhouseSourceSplitCoordinator(SourceSplitCoordinator.Context&lt;ClickhouseSourceSplit, EmptyState&gt; context,
                                  BitSailConfiguration jobConf) {
  this.context = context;
  this.jobConf = jobConf;
  this.splitAssignmentPlan = Maps.newConcurrentMap();
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p>In the scenario where State is customized, it is necessary to save and restore the state stored in <code>SourceSplitCoordinator.Context</code> during checkpoint.</p><p>Take RocketMQSourceSplitCoordinator as an example:</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public RocketMQSourceSplitCoordinator(
    SourceSplitCoordinator.Context&lt;RocketMQSplit, RocketMQState&gt; context,
    BitSailConfiguration jobConfiguration,
    Boundedness boundedness) {
  this.context = context;
  this.jobConfiguration = jobConfiguration;
  this.boundedness = boundedness;
  this.discoveryInternal = jobConfiguration.get(RocketMQSourceOptions.DISCOVERY_INTERNAL);
  this.pendingRocketMQSplitAssignment = Maps.newConcurrentMap();

  this.discoveredPartitions = new HashSet&lt;&gt;();
  if (context.isRestored()) {
    RocketMQState restoreState = context.getRestoreState();
    assignedPartitions = restoreState.getAssignedWithSplits();
    discoveredPartitions.addAll(assignedPartitions.keySet());
  } else {
    assignedPartitions = Maps.newHashMap();
  }

  prepareConsumerProperties();
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="start-method" tabindex="-1"><a class="header-anchor" href="#start-method" aria-hidden="true">#</a> start method</h3><p>Extract split metadata required by some data sources.</p><h4 id="unified-example-2" tabindex="-1"><a class="header-anchor" href="#unified-example-2" aria-hidden="true">#</a> Unified example</h4><p>Take RocketMQSourceSplitCoordinator as an example:</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>private void prepareRocketMQConsumer() {
  try {
    consumer = RocketMQUtils.prepareRocketMQConsumer(jobConfiguration,
        String.format(COORDINATOR_INSTANCE_NAME_TEMPLATE,
            cluster, topic, consumerGroup, UUID.randomUUID()));
    consumer.start();
  } catch (Exception e) {
    throw BitSailException.asBitSailException(RocketMQErrorCode.CONSUMER_CREATE_FAILED, e);
  }
}

@Override
public void start() {
  prepareRocketMQConsumer();
  splitAssigner = new FairRocketMQSplitAssigner(jobConfiguration, assignedPartitions);
  if (discoveryInternal &gt; 0) {
    context.runAsync(
        this::fetchMessageQueues,
        this::handleMessageQueueChanged,
        0,
        discoveryInternal
    );
  } else {
    context.runAsyncOnce(
        this::fetchMessageQueues,
        this::handleMessageQueueChanged
    );
  }
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h4 id="batch-example-2" tabindex="-1"><a class="header-anchor" href="#batch-example-2" aria-hidden="true">#</a> Batch example</h4><p>Take ClickhouseSourceSplitCoordinator as an example:</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public void start() {
  List&lt;ClickhouseSourceSplit&gt; splitList;
  try {
    SimpleDivideSplitConstructor constructor = new SimpleDivideSplitConstructor(jobConf);
    splitList = constructor.construct();
  } catch (IOException e) {
    ClickhouseSourceSplit split = new ClickhouseSourceSplit(0);
    split.setReadTable(true);
    splitList = Collections.singletonList(split);
    LOG.error(&quot;Failed to construct splits, will directly read the table.&quot;, e);
  }

  int readerNum = context.totalParallelism();
  LOG.info(&quot;Found {} readers and {} splits.&quot;, readerNum, splitList.size());
  if (readerNum &gt; splitList.size()) {
    LOG.error(&quot;Reader number {} is larger than split number {}.&quot;, readerNum, splitList.size());
  }

  for (ClickhouseSourceSplit split : splitList) {
    int readerIndex = ReaderSelector.getReaderIndex(readerNum);
    splitAssignmentPlan.computeIfAbsent(readerIndex, k -&gt; new HashSet&lt;&gt;()).add(split);
    LOG.info(&quot;Will assign split {} to the {}-th reader&quot;, split.uniqSplitId(), readerIndex);
  }
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="assigner" tabindex="-1"><a class="header-anchor" href="#assigner" aria-hidden="true">#</a> Assigner</h3><p>Assign the divided splits to the Reader. During the development process, we usually let the SourceSplitCoordinator focus on processing the communication with the Reader. The actual split distribution logic is generally encapsulated in the Assigner. This Assigner can be an encapsulated <code>Split Assign function</code>, or it can be a <code>Split Assigner class</code>.</p><h4 id="assign-function-example" tabindex="-1"><a class="header-anchor" href="#assign-function-example" aria-hidden="true">#</a> Assign function example</h4><p>Take ClickhouseSourceSplitCoordinator as an example:</p><p>The <code>tryAssignSplitsToReader</code> function assigns the divided slices stored in <code>splitAssignmentPlan</code> to the corresponding Reader.</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>private void tryAssignSplitsToReader() {
  Map&lt;Integer, List&lt;ClickhouseSourceSplit&gt;&gt; splitsToAssign = new HashMap&lt;&gt;();

  for (Integer readerIndex : splitAssignmentPlan.keySet()) {
    if (CollectionUtils.isNotEmpty(splitAssignmentPlan.get(readerIndex)) &amp;&amp; context.registeredReaders().contains(readerIndex)) {
      splitsToAssign.put(readerIndex, Lists.newArrayList(splitAssignmentPlan.get(readerIndex)));
    }
  }

  for (Integer readerIndex : splitsToAssign.keySet()) {
    LOG.info(&quot;Try assigning splits reader {}, splits are: [{}]&quot;, readerIndex,
        splitsToAssign.get(readerIndex).stream().map(ClickhouseSourceSplit::uniqSplitId).collect(Collectors.toList()));
    splitAssignmentPlan.remove(readerIndex);
    context.assignSplit(readerIndex, splitsToAssign.get(readerIndex));
    context.signalNoMoreSplits(readerIndex);
    LOG.info(&quot;Finish assigning splits reader {}&quot;, readerIndex);
  }
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h4 id="assigner-class-example" tabindex="-1"><a class="header-anchor" href="#assigner-class-example" aria-hidden="true">#</a> Assigner class example</h4><p>Take RocketMQSourceSplitCoordinator as an example:</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public class FairRocketMQSplitAssigner implements SplitAssigner&lt;MessageQueue&gt; {

  private BitSailConfiguration readerConfiguration;

  private AtomicInteger atomicInteger;

  public Map&lt;MessageQueue, String&gt; rocketMQSplitIncrementMapping;

  public FairRocketMQSplitAssigner(BitSailConfiguration readerConfiguration,
                                   Map&lt;MessageQueue, String&gt; rocketMQSplitIncrementMapping) {
    this.readerConfiguration = readerConfiguration;
    this.rocketMQSplitIncrementMapping = rocketMQSplitIncrementMapping;
    this.atomicInteger = new AtomicInteger(CollectionUtils
        .size(rocketMQSplitIncrementMapping.keySet()));
  }

  @Override
  public String assignSplitId(MessageQueue messageQueue) {
    if (!rocketMQSplitIncrementMapping.containsKey(messageQueue)) {
      rocketMQSplitIncrementMapping.put(messageQueue, String.valueOf(atomicInteger.getAndIncrement()));
    }
    return rocketMQSplitIncrementMapping.get(messageQueue);
  }

  @Override
  public int assignToReader(String splitId, int totalParallelism) {
    return splitId.hashCode() % totalParallelism;
  }
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="addreader-method" tabindex="-1"><a class="header-anchor" href="#addreader-method" aria-hidden="true">#</a> addReader method</h3><p>Call Assigner to add splits to Reader.</p><h4 id="batch-example-3" tabindex="-1"><a class="header-anchor" href="#batch-example-3" aria-hidden="true">#</a> Batch example</h4><p>Take ClickhouseSourceSplitCoordinator as an example:</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public void addReader(int subtaskId) {
  LOG.info(&quot;Found reader {}&quot;, subtaskId);
  tryAssignSplitsToReader();
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h4 id="unified-example-3" tabindex="-1"><a class="header-anchor" href="#unified-example-3" aria-hidden="true">#</a> Unified example</h4><p>Take RocketMQSourceSplitCoordinator as an example:</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>private void notifyReaderAssignmentResult() {
  Map&lt;Integer, List&lt;RocketMQSplit&gt;&gt; tmpRocketMQSplitAssignments = new HashMap&lt;&gt;();

  for (Integer pendingAssignmentReader : pendingRocketMQSplitAssignment.keySet()) {

    if (CollectionUtils.isNotEmpty(pendingRocketMQSplitAssignment.get(pendingAssignmentReader))
        &amp;&amp; context.registeredReaders().contains(pendingAssignmentReader)) {

      tmpRocketMQSplitAssignments.put(pendingAssignmentReader, Lists.newArrayList(pendingRocketMQSplitAssignment.get(pendingAssignmentReader)));
    }
  }

  for (Integer pendingAssignmentReader : tmpRocketMQSplitAssignments.keySet()) {

    LOG.info(&quot;Assigning splits to reader {}, splits = {}.&quot;, pendingAssignmentReader,
        tmpRocketMQSplitAssignments.get(pendingAssignmentReader));

    context.assignSplit(pendingAssignmentReader,
        tmpRocketMQSplitAssignments.get(pendingAssignmentReader));
    Set&lt;RocketMQSplit&gt; removes = pendingRocketMQSplitAssignment.remove(pendingAssignmentReader);
    removes.forEach(removeSplit -&gt; {
      assignedPartitions.put(removeSplit.getMessageQueue(), removeSplit.getSplitId());
    });

    LOG.info(&quot;Assigned splits to reader {}&quot;, pendingAssignmentReader);

    if (Boundedness.BOUNDEDNESS == boundedness) {
      LOG.info(&quot;Signal reader {} no more splits assigned in future.&quot;, pendingAssignmentReader);
      context.signalNoMoreSplits(pendingAssignmentReader);
    }
  }
}

@Override
public void addReader(int subtaskId) {
  LOG.info(
      &quot;Adding reader {} to RocketMQ Split Coordinator for consumer group {}.&quot;,
      subtaskId,
      consumerGroup);
  notifyReaderAssignmentResult();
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="addsplitsback-method" tabindex="-1"><a class="header-anchor" href="#addsplitsback-method" aria-hidden="true">#</a> addSplitsBack method</h3><p>For some splits that have not been processed by the Reader, reassign them. The reassignment strategy can be defined by yourself. The common strategy is hash modulo. All the Splits in the returned Split list are reassigned and then assigned to different Readers.</p><h4 id="batch-example-4" tabindex="-1"><a class="header-anchor" href="#batch-example-4" aria-hidden="true">#</a> Batch example</h4><p>以ClickhouseSourceSplitCoordinator为例：</p><p><code>ReaderSelector</code> uses the hash modulo strategy to redistribute the Split list.</p><p>The tryAssignSplitsToReader method assigns the reassigned Split collection to Reader through Assigner.</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public void addSplitsBack(List&lt;ClickhouseSourceSplit&gt; splits, int subtaskId) {
  LOG.info(&quot;Source reader {} return splits {}.&quot;, subtaskId, splits);

  int readerNum = context.totalParallelism();
  for (ClickhouseSourceSplit split : splits) {
    int readerIndex = ReaderSelector.getReaderIndex(readerNum);
    splitAssignmentPlan.computeIfAbsent(readerIndex, k -&gt; new HashSet&lt;&gt;()).add(split);
    LOG.info(&quot;Re-assign split {} to the {}-th reader.&quot;, split.uniqSplitId(), readerIndex);
  }

  tryAssignSplitsToReader();
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h4 id="unified-example-4" tabindex="-1"><a class="header-anchor" href="#unified-example-4" aria-hidden="true">#</a> Unified example</h4><p>Take RocketMQSourceSplitCoordinator as an example:</p><p><code>addSplitChangeToPendingAssignment</code> uses the hash modulo strategy to reassign the Split list.</p><p><code>notifyReaderAssignmentResult</code> assigns the reassigned Split collection to Reader through Assigner.</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>private synchronized void addSplitChangeToPendingAssignment(Set&lt;RocketMQSplit&gt; newRocketMQSplits) {
  int numReader = context.totalParallelism();
  for (RocketMQSplit split : newRocketMQSplits) {
    int readerIndex = splitAssigner.assignToReader(split.getSplitId(), numReader);
    pendingRocketMQSplitAssignment.computeIfAbsent(readerIndex, r -&gt; new HashSet&lt;&gt;())
        .add(split);
  }
  LOG.debug(&quot;RocketMQ splits {} finished assignment.&quot;, newRocketMQSplits);
}

@Override
public void addSplitsBack(List&lt;RocketMQSplit&gt; splits, int subtaskId) {
  LOG.info(&quot;Source reader {} return splits {}.&quot;, subtaskId, splits);
  addSplitChangeToPendingAssignment(new HashSet&lt;&gt;(splits));
  notifyReaderAssignmentResult();
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="snapshotstate-method" tabindex="-1"><a class="header-anchor" href="#snapshotstate-method" aria-hidden="true">#</a> snapshotState method</h3><p>Store the snapshot information of the processing split, which is used in the construction method when restoring.</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public RocketMQState snapshotState() throws Exception {
  return new RocketMQState(assignedPartitions);
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="close-method" tabindex="-1"><a class="header-anchor" href="#close-method" aria-hidden="true">#</a> close method</h3><p>Closes all open connectors that interact with the data source to read metadata information during split method.</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public void close() {
  if (consumer != null) {
    consumer.shutdown();
  }
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h2 id="sourcereader" tabindex="-1"><a class="header-anchor" href="#sourcereader" aria-hidden="true">#</a> SourceReader</h2><p>Each SourceReader is executed in an independent thread. As long as we ensure that the slices assigned by SourceSplitCoordinator to different SourceReaders have no intersection, we can ignore any concurrency details during the execution cycle of SourceReader.</p><p><img src="`+o+`" alt="" loading="lazy"></p><h3 id="sourcereader-interface" tabindex="-1"><a class="header-anchor" href="#sourcereader-interface" aria-hidden="true">#</a> SourceReader Interface</h3><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public interface SourceReader&lt;T, SplitT extends SourceSplit&gt; extends Serializable, AutoCloseable {

  void start();

  void pollNext(SourcePipeline&lt;T&gt; pipeline) throws Exception;

  void addSplits(List&lt;SplitT&gt; splits);

  /**
   * Check source reader has more elements or not.
   */
  boolean hasMoreElements();

  /**
   * There will no more split will send to this source reader.
   * Source reader could be exited after process all assigned split.
   */
  default void notifyNoMoreSplits() {

  }

  /**
   * Process all events which from {@link SourceSplitCoordinator}.
   */
  default void handleSourceEvent(SourceEvent sourceEvent) {
  }

  /**
   * Store the split to the external system to recover when task failed.
   */
  List&lt;SplitT&gt; snapshotState(long checkpointId);

  /**
   * When all tasks finished snapshot, notify checkpoint complete will be invoked.
   */
  default void notifyCheckpointComplete(long checkpointId) throws Exception {

  }

  interface Context {

    TypeInfo&lt;?&gt;[] getTypeInfos();

    String[] getFieldNames();

    int getIndexOfSubtask();

    void sendSplitRequest();
  }
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="construction-method-1" tabindex="-1"><a class="header-anchor" href="#construction-method-1" aria-hidden="true">#</a> Construction method</h3><p>Here it is necessary to complete the extraction of various configurations related to data source access, such as database name table name, message queue cluster and topic, identity authentication configuration, and so on.</p><h4 id="example-6" tabindex="-1"><a class="header-anchor" href="#example-6" aria-hidden="true">#</a> example</h4><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public RocketMQSourceReader(BitSailConfiguration readerConfiguration,
                            Context context,
                            Boundedness boundedness) {
  this.readerConfiguration = readerConfiguration;
  this.boundedness = boundedness;
  this.context = context;
  this.assignedRocketMQSplits = Sets.newHashSet();
  this.finishedRocketMQSplits = Sets.newHashSet();
  this.deserializationSchema = new RocketMQDeserializationSchema(
      readerConfiguration,
      context.getTypeInfos(),
      context.getFieldNames());
  this.noMoreSplits = false;

  cluster = readerConfiguration.get(RocketMQSourceOptions.CLUSTER);
  topic = readerConfiguration.get(RocketMQSourceOptions.TOPIC);
  consumerGroup = readerConfiguration.get(RocketMQSourceOptions.CONSUMER_GROUP);
  consumerTag = readerConfiguration.get(RocketMQSourceOptions.CONSUMER_TAG);
  pollBatchSize = readerConfiguration.get(RocketMQSourceOptions.POLL_BATCH_SIZE);
  pollTimeout = readerConfiguration.get(RocketMQSourceOptions.POLL_TIMEOUT);
  commitInCheckpoint = readerConfiguration.get(RocketMQSourceOptions.COMMIT_IN_CHECKPOINT);
  accessKey = readerConfiguration.get(RocketMQSourceOptions.ACCESS_KEY);
  secretKey = readerConfiguration.get(RocketMQSourceOptions.SECRET_KEY);
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="start-method-1" tabindex="-1"><a class="header-anchor" href="#start-method-1" aria-hidden="true">#</a> start method</h3><p>Obtain the access object of the data source, such as the execution object of the database, the consumer object of the message queue, or the recordReader object of the file system.</p><h4 id="example-7" tabindex="-1"><a class="header-anchor" href="#example-7" aria-hidden="true">#</a> example</h4><p>Message queue</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public void start() {
  try {
    if (StringUtils.isNotEmpty(accessKey) &amp;&amp; StringUtils.isNotEmpty(secretKey)) {
      AclClientRPCHook aclClientRPCHook = new AclClientRPCHook(
          new SessionCredentials(accessKey, secretKey));
      consumer = new DefaultMQPullConsumer(aclClientRPCHook);
    } else {
      consumer = new DefaultMQPullConsumer();
    }

    consumer.setConsumerGroup(consumerGroup);
    consumer.setNamesrvAddr(cluster);
    consumer.setInstanceName(String.format(SOURCE_READER_INSTANCE_NAME_TEMPLATE,
        cluster, topic, consumerGroup, UUID.randomUUID()));
    consumer.setConsumerPullTimeoutMillis(pollTimeout);
    consumer.start();
  } catch (Exception e) {
    throw BitSailException.asBitSailException(RocketMQErrorCode.CONSUMER_CREATE_FAILED, e);
  }
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p>Database</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public void start() {
  this.connection = connectionHolder.connect();

  // Construct statement.
  String baseSql = ClickhouseJdbcUtils.getQuerySql(dbName, tableName, columnInfos);
  String querySql = ClickhouseJdbcUtils.decorateSql(baseSql, splitField, filterSql, maxFetchCount, true);
  try {
    this.statement = connection.prepareStatement(querySql);
  } catch (SQLException e) {
    throw new RuntimeException(&quot;Failed to prepare statement.&quot;, e);
  }

  LOG.info(&quot;Task {} started.&quot;, subTaskId);
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p>FTP</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public void start() {
  this.ftpHandler.loginFtpServer();
  if (this.ftpHandler.getFtpConfig().getSkipFirstLine()) {
    this.skipFirstLine = true;
  }
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="addsplits-method" tabindex="-1"><a class="header-anchor" href="#addsplits-method" aria-hidden="true">#</a> addSplits method</h3><p>Add the Splits list assigned by SourceSplitCoordinator to the current Reader to its own processing queue or set.</p><h4 id="example-8" tabindex="-1"><a class="header-anchor" href="#example-8" aria-hidden="true">#</a> example</h4><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public void addSplits(List&lt;RocketMQSplit&gt; splits) {
  LOG.info(&quot;Subtask {} received {}(s) new splits, splits = {}.&quot;,
      context.getIndexOfSubtask(),
      CollectionUtils.size(splits),
      splits);

  assignedRocketMQSplits.addAll(splits);
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="hasmoreelements-method" tabindex="-1"><a class="header-anchor" href="#hasmoreelements-method" aria-hidden="true">#</a> hasMoreElements method</h3><p>In an unbounded stream computing scenario, it will always return true to ensure that the Reader thread is not destroyed.</p><p>In a batch scenario, false will be returned after the slices assigned to the Reader are processed, indicating the end of the Reader&#39;s life cycle.</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public boolean hasMoreElements() {
  if (boundedness == Boundedness.UNBOUNDEDNESS) {
    return true;
  }
  if (noMoreSplits) {
    return CollectionUtils.size(assignedRocketMQSplits) != 0;
  }
  return true;
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="pollnext-method" tabindex="-1"><a class="header-anchor" href="#pollnext-method" aria-hidden="true">#</a> pollNext method</h3><p>When the addSplits method adds the slice processing queue and hasMoreElements returns true, this method is called, and the developer implements this method to actually interact with the data.</p><p>Developers need to pay attention to the following issues when implementing the pollNext method:</p><ul><li>Reading of split data <ul><li>Read data from the constructed split.</li></ul></li><li>Conversion of data types <ul><li>Convert external data to BitSail&#39;s Row type</li></ul></li></ul><h4 id="example-9" tabindex="-1"><a class="header-anchor" href="#example-9" aria-hidden="true">#</a> example</h4><p>Take RocketMQSourceReader as an example:</p><p>Select a split from the split queue for processing, read its information, and then convert the read information into <code>BitSail&#39;s Row type</code> and send it downstream for processing.</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public void pollNext(SourcePipeline&lt;Row&gt; pipeline) throws Exception {
  for (RocketMQSplit rocketmqSplit : assignedRocketMQSplits) {
    MessageQueue messageQueue = rocketmqSplit.getMessageQueue();
    PullResult pullResult = consumer.pull(rocketmqSplit.getMessageQueue(),
        consumerTag,
        rocketmqSplit.getStartOffset(),
        pollBatchSize,
        pollTimeout);

    if (Objects.isNull(pullResult) || CollectionUtils.isEmpty(pullResult.getMsgFoundList())) {
      continue;
    }

    for (MessageExt message : pullResult.getMsgFoundList()) {
      Row deserialize = deserializationSchema.deserialize(message.getBody());
      pipeline.output(deserialize);
      if (rocketmqSplit.getStartOffset() &gt;= rocketmqSplit.getEndOffset()) {
        LOG.info(&quot;Subtask {} rocketmq split {} in end of stream.&quot;,
            context.getIndexOfSubtask(),
            rocketmqSplit);
        finishedRocketMQSplits.add(rocketmqSplit);
        break;
      }
    }
    rocketmqSplit.setStartOffset(pullResult.getNextBeginOffset());
    if (!commitInCheckpoint) {
      consumer.updateConsumeOffset(messageQueue, pullResult.getMaxOffset());
    }
  }
  assignedRocketMQSplits.removeAll(finishedRocketMQSplits);
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h4 id="the-way-to-convert-to-bitsail-row-type" tabindex="-1"><a class="header-anchor" href="#the-way-to-convert-to-bitsail-row-type" aria-hidden="true">#</a> The way to convert to BitSail Row type</h4><h5 id="rowdeserializer-class" tabindex="-1"><a class="header-anchor" href="#rowdeserializer-class" aria-hidden="true">#</a> RowDeserializer class</h5><p>Apply different converters to columns of different formats, and set them to the <code>Field</code> of the corresponding <code>Row Field</code>.</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public class ClickhouseRowDeserializer {

  interface FiledConverter {
    Object apply(ResultSet resultSet) throws SQLException;
  }

  private final List&lt;FiledConverter&gt; converters;
  private final int fieldSize;

  public ClickhouseRowDeserializer(TypeInfo&lt;?&gt;[] typeInfos) {
    this.fieldSize = typeInfos.length;
    this.converters = new ArrayList&lt;&gt;();
    for (int i = 0; i &lt; fieldSize; ++i) {
      converters.add(initFieldConverter(i + 1, typeInfos[i]));
    }
  }

  public Row convert(ResultSet resultSet) {
    Row row = new Row(fieldSize);
    try {
      for (int i = 0; i &lt; fieldSize; ++i) {
        row.setField(i, converters.get(i).apply(resultSet));
      }
    } catch (SQLException e) {
      throw BitSailException.asBitSailException(ClickhouseErrorCode.CONVERT_ERROR, e.getCause());
    }
    return row;
  }

  private FiledConverter initFieldConverter(int index, TypeInfo&lt;?&gt; typeInfo) {
    if (!(typeInfo instanceof BasicTypeInfo)) {
      throw BitSailException.asBitSailException(CommonErrorCode.UNSUPPORTED_COLUMN_TYPE, typeInfo.getTypeClass().getName() + &quot; is not supported yet.&quot;);
    }

    Class&lt;?&gt; curClass = typeInfo.getTypeClass();
    if (TypeInfos.BYTE_TYPE_INFO.getTypeClass() == curClass) {
      return resultSet -&gt; resultSet.getByte(index);
    }
    if (TypeInfos.SHORT_TYPE_INFO.getTypeClass() == curClass) {
      return resultSet -&gt; resultSet.getShort(index);
    }
    if (TypeInfos.INT_TYPE_INFO.getTypeClass() == curClass) {
      return resultSet -&gt; resultSet.getInt(index);
    }
    if (TypeInfos.LONG_TYPE_INFO.getTypeClass() == curClass) {
      return resultSet -&gt; resultSet.getLong(index);
    }
    if (TypeInfos.BIG_INTEGER_TYPE_INFO.getTypeClass() == curClass) {
      return resultSet -&gt; {
        BigDecimal dec = resultSet.getBigDecimal(index);
        return dec == null ? null : dec.toBigInteger();
      };
    }
    if (TypeInfos.FLOAT_TYPE_INFO.getTypeClass() == curClass) {
      return resultSet -&gt; resultSet.getFloat(index);
    }
    if (TypeInfos.DOUBLE_TYPE_INFO.getTypeClass() == curClass) {
      return resultSet -&gt; resultSet.getDouble(index);
    }
    if (TypeInfos.BIG_DECIMAL_TYPE_INFO.getTypeClass() == curClass) {
      return resultSet -&gt; resultSet.getBigDecimal(index);
    }
    if (TypeInfos.STRING_TYPE_INFO.getTypeClass() == curClass) {
      return resultSet -&gt; resultSet.getString(index);
    }
    if (TypeInfos.SQL_DATE_TYPE_INFO.getTypeClass() == curClass) {
      return resultSet -&gt; resultSet.getDate(index);
    }
    if (TypeInfos.SQL_TIMESTAMP_TYPE_INFO.getTypeClass() == curClass) {
      return resultSet -&gt; resultSet.getTimestamp(index);
    }
    if (TypeInfos.SQL_TIME_TYPE_INFO.getTypeClass() == curClass) {
      return resultSet -&gt; resultSet.getTime(index);
    }
    if (TypeInfos.BOOLEAN_TYPE_INFO.getTypeClass() == curClass) {
      return resultSet -&gt; resultSet.getBoolean(index);
    }
    if (TypeInfos.VOID_TYPE_INFO.getTypeClass() == curClass) {
      return resultSet -&gt; null;
    }
    throw new UnsupportedOperationException(&quot;Unsupported data type: &quot; + typeInfo);
  }
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h5 id="implement-the-deserializationschema-interface" tabindex="-1"><a class="header-anchor" href="#implement-the-deserializationschema-interface" aria-hidden="true">#</a> Implement the DeserializationSchema interface</h5><p>Compared with implementing <code>RowDeserializer</code>, we hope that you can implement an implementation class that inherits the <code>DeserializationSchema</code> interface, and convert data in a certain format, such as <code>JSON</code> and <code>CSV</code>, into <code>BitSail Row type</code>.</p><p><img src="`+c+`" alt="" loading="lazy"></p><p>In specific applications, we can use a unified interface to create corresponding implementation classes.</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public class TextInputFormatDeserializationSchema implements DeserializationSchema&lt;Writable, Row&gt; {

  private BitSailConfiguration deserializationConfiguration;

  private TypeInfo&lt;?&gt;[] typeInfos;

  private String[] fieldNames;

  private transient DeserializationSchema&lt;byte[], Row&gt; deserializationSchema;

  public TextInputFormatDeserializationSchema(BitSailConfiguration deserializationConfiguration,
                                               TypeInfo&lt;?&gt;[] typeInfos,
                                               String[] fieldNames) {
    this.deserializationConfiguration = deserializationConfiguration;
    this.typeInfos = typeInfos;
    this.fieldNames = fieldNames;
    ContentType contentType = ContentType.valueOf(
        deserializationConfiguration.getNecessaryOption(HadoopReaderOptions.CONTENT_TYPE, HadoopErrorCode.REQUIRED_VALUE).toUpperCase());
    switch (contentType) {
      case CSV:
        this.deserializationSchema =
            new CsvDeserializationSchema(deserializationConfiguration, typeInfos, fieldNames);
        break;
      case JSON:
        this.deserializationSchema =
            new JsonDeserializationSchema(deserializationConfiguration, typeInfos, fieldNames);
        break;
      default:
        throw BitSailException.asBitSailException(HadoopErrorCode.UNSUPPORTED_ENCODING, &quot;unsupported parser type: &quot; + contentType);
    }
  }

  @Override
  public Row deserialize(Writable message) {
    return deserializationSchema.deserialize((message.toString()).getBytes());
  }

  @Override
  public boolean isEndOfStream(Row nextElement) {
    return false;
  }
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p>You can also customize the <code>DeserializationSchema</code> that currently needs to be parsed:</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public class MapredParquetInputFormatDeserializationSchema implements DeserializationSchema&lt;Writable, Row&gt; {

  private final BitSailConfiguration deserializationConfiguration;

  private final transient DateTimeFormatter localDateTimeFormatter;
  private final transient DateTimeFormatter localDateFormatter;
  private final transient DateTimeFormatter localTimeFormatter;
  private final int fieldSize;
  private final TypeInfo&lt;?&gt;[] typeInfos;
  private final String[] fieldNames;
  private final List&lt;DeserializationConverter&gt; converters;

  public MapredParquetInputFormatDeserializationSchema(BitSailConfiguration deserializationConfiguration,
                                  TypeInfo&lt;?&gt;[] typeInfos,
                                  String[] fieldNames) {

    this.deserializationConfiguration = deserializationConfiguration;
    this.typeInfos = typeInfos;
    this.fieldNames = fieldNames;
    this.localDateTimeFormatter = DateTimeFormatter.ofPattern(
        deserializationConfiguration.get(CommonOptions.DateFormatOptions.DATE_TIME_PATTERN));
    this.localDateFormatter = DateTimeFormatter
        .ofPattern(deserializationConfiguration.get(CommonOptions.DateFormatOptions.DATE_PATTERN));
    this.localTimeFormatter = DateTimeFormatter
        .ofPattern(deserializationConfiguration.get(CommonOptions.DateFormatOptions.TIME_PATTERN));
    this.fieldSize = typeInfos.length;
    this.converters = Arrays.stream(typeInfos).map(this::createTypeInfoConverter).collect(Collectors.toList());
  }

  @Override
  public Row deserialize(Writable message) {
    int arity = fieldNames.length;
    Row row = new Row(arity);
    Writable[] writables = ((ArrayWritable) message).get();
    for (int i = 0; i &lt; fieldSize; ++i) {
      row.setField(i, converters.get(i).convert(writables[i].toString()));
    }
    return row;
  }

  @Override
  public boolean isEndOfStream(Row nextElement) {
    return false;
  }

  private interface DeserializationConverter extends Serializable {
    Object convert(String input);
  }

  private DeserializationConverter createTypeInfoConverter(TypeInfo&lt;?&gt; typeInfo) {
    Class&lt;?&gt; typeClass = typeInfo.getTypeClass();

    if (typeClass == TypeInfos.VOID_TYPE_INFO.getTypeClass()) {
      return field -&gt; null;
    }
    if (typeClass == TypeInfos.BOOLEAN_TYPE_INFO.getTypeClass()) {
      return this::convertToBoolean;
    }
    if (typeClass == TypeInfos.INT_TYPE_INFO.getTypeClass()) {
      return this::convertToInt;
    }
    throw BitSailException.asBitSailException(CsvFormatErrorCode.CSV_FORMAT_COVERT_FAILED,
        String.format(&quot;Csv format converter not support type info: %s.&quot;, typeInfo));
  }

  private boolean convertToBoolean(String field) {
    return Boolean.parseBoolean(field.trim());
  }

  private int convertToInt(String field) {
    return Integer.parseInt(field.trim());
  }
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="snapshotstate-method-1" tabindex="-1"><a class="header-anchor" href="#snapshotstate-method-1" aria-hidden="true">#</a> snapshotState method</h3><p>Generate and save the snapshot information of State for <code>checkpoint</code>.</p><h4 id="example-10" tabindex="-1"><a class="header-anchor" href="#example-10" aria-hidden="true">#</a> example</h4><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public List&lt;RocketMQSplit&gt; snapshotState(long checkpointId) {
  LOG.info(&quot;Subtask {} start snapshotting for checkpoint id = {}.&quot;, context.getIndexOfSubtask(), checkpointId);
  if (commitInCheckpoint) {
    for (RocketMQSplit rocketMQSplit : assignedRocketMQSplits) {
      try {
        consumer.updateConsumeOffset(rocketMQSplit.getMessageQueue(), rocketMQSplit.getStartOffset());
        LOG.debug(&quot;Subtask {} committed message queue = {} in checkpoint id = {}.&quot;, context.getIndexOfSubtask(),
            rocketMQSplit.getMessageQueue(),
            checkpointId);
      } catch (MQClientException e) {
        throw new RuntimeException(e);
      }
    }
  }
  return Lists.newArrayList(assignedRocketMQSplits);
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="hasmoreelements-method-1" tabindex="-1"><a class="header-anchor" href="#hasmoreelements-method-1" aria-hidden="true">#</a> hasMoreElements method</h3><p>The <code>sourceReader.hasMoreElements()</code> judgment will be made before calling the <code>pollNext</code> method each time. If and only if the judgment passes, the <code>pollNext</code> method will be called.</p><h4 id="example-11" tabindex="-1"><a class="header-anchor" href="#example-11" aria-hidden="true">#</a> example</h4><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public boolean hasMoreElements() {
  if (noMoreSplits) {
    return CollectionUtils.size(assignedHadoopSplits) != 0;
  }
  return true;
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="notifynomoresplits-method" tabindex="-1"><a class="header-anchor" href="#notifynomoresplits-method" aria-hidden="true">#</a> notifyNoMoreSplits method</h3><p>This method is called when the Reader has processed all splits.</p><h4 id="example-12" tabindex="-1"><a class="header-anchor" href="#example-12" aria-hidden="true">#</a> example</h4><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public void notifyNoMoreSplits() {
  LOG.info(&quot;Subtask {} received no more split signal.&quot;, context.getIndexOfSubtask());
  noMoreSplits = true;
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div>`,206);function C(x,I){const n=g("RouterLink");return v(),m("div",null,[S,e("p",null,[i("English | "),p(n,{to:"/zh/community/source_connector_detail.html"},{default:b(()=>[i("简体中文")]),_:1})]),y])}const E=u(f,[["render",C],["__file","source_connector_detail.html.vue"]]);export{E as default};
