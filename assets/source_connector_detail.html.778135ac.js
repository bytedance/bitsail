import{_ as s}from"./bitsail_model.b409088b.js";import{_ as l,a,b as d,c as r,d as t,e as v,f as u}from"./deserialization_schema_diagram.b71ffa52.js";import{_ as c}from"./_plugin-vue_export-helper.cdc0426e.js";import{o,c as m,a as e,d as b,w as p,b as i,e as g,r as h}from"./app.7342ef47.js";const S={},f=e("h1",{id:"source-connector-详解",tabindex:"-1"},[e("a",{class:"header-anchor",href:"#source-connector-详解","aria-hidden":"true"},"#"),i(" Source Connector 详解")],-1),C=g('<hr><h2 id="bitsail-source-connector交互流程介绍" tabindex="-1"><a class="header-anchor" href="#bitsail-source-connector交互流程介绍" aria-hidden="true">#</a> BitSail Source Connector交互流程介绍</h2><p><img src="'+s+'" alt="" loading="lazy"></p><ul><li>Source: 参与数据读取组件的生命周期管理，主要负责和框架的交互，构架作业，不参与作业真正的执行。</li><li>SourceSplit: 数据读取分片，大数据处理框架的核心目的就是将大规模的数据拆分成为多个合理的Split并行处理。</li><li>State：作业状态快照，当开启checkpoint之后，会保存当前执行状态。</li><li>SplitCoordinator: SplitCoordinator承担创建、管理Split的角色。</li><li>SourceReader: 真正负责数据读取的组件，在接收到Split后会对其进行数据读取，然后将数据传输给下一个算子。</li></ul><h2 id="source" tabindex="-1"><a class="header-anchor" href="#source" aria-hidden="true">#</a> Source</h2><p>数据读取组件的生命周期管理，主要负责和框架的交互，构架作业，它不参与作业真正的执行。</p><p>以RocketMQSource为例：Source方法需要实现Source和ParallelismComputable接口。</p><p><img src="'+l+`" alt="" loading="lazy"></p><h3 id="source接口" tabindex="-1"><a class="header-anchor" href="#source接口" aria-hidden="true">#</a> Source接口</h3><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public interface Source&lt;T, SplitT extends SourceSplit, StateT extends Serializable&gt;
    extends Serializable, TypeInfoConverterFactory {

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
   * Get Source&#39; name.
   */
  String getReaderName();
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h4 id="configure方法" tabindex="-1"><a class="header-anchor" href="#configure方法" aria-hidden="true">#</a> configure方法</h4><p>主要去做一些客户端的配置的分发和提取，可以操作运行时环境ExecutionEnviron的配置和readerConfiguration的配置。</p><h5 id="示例" tabindex="-1"><a class="header-anchor" href="#示例" aria-hidden="true">#</a> 示例</h5><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>@Override
public void configure(ExecutionEnviron execution, BitSailConfiguration readerConfiguration) {
  this.readerConfiguration = readerConfiguration;
  this.commonConfiguration = execution.getCommonConfiguration();
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h4 id="getsourceboundedness方法" tabindex="-1"><a class="header-anchor" href="#getsourceboundedness方法" aria-hidden="true">#</a> getSourceBoundedness方法</h4><p>设置作业的处理方式，是采用流式处理方法、批式处理方法，或者是流批一体的处理方式，在流批一体的场景中，我们需要根据作业的不同类型设置不同的处理方式。</p><p>具体对应关系如下：</p><table><thead><tr><th>Job Type</th><th>Boundedness</th></tr></thead><tbody><tr><td>batch</td><td>Boundedness.<em>BOUNDEDNESS</em></td></tr><tr><td>stream</td><td>Boundedness.<em>UNBOUNDEDNESS</em></td></tr></tbody></table><h5 id="流批一体场景示例" tabindex="-1"><a class="header-anchor" href="#流批一体场景示例" aria-hidden="true">#</a> 流批一体场景示例</h5><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>@Override
public Boundedness getSourceBoundedness() {
  return Mode.BATCH.equals(Mode.getJobRunMode(commonConfiguration.get(CommonOptions.JOB_TYPE))) ?
      Boundedness.BOUNDEDNESS :
      Boundedness.UNBOUNDEDNESS;
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h5 id="批式场景示例" tabindex="-1"><a class="header-anchor" href="#批式场景示例" aria-hidden="true">#</a> 批式场景示例</h5><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public Boundedness getSourceBoundedness() {
  return Boundedness.BOUNDEDNESS;
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h4 id="createtypeinfoconverter方法" tabindex="-1"><a class="header-anchor" href="#createtypeinfoconverter方法" aria-hidden="true">#</a> createTypeInfoConverter方法</h4><p>用于指定Source连接器的类型转换器；我们知道大多数的外部数据系统都存在着自己的类型定义，它们的定义与BitSail的类型定义不会完全一致；为了简化类型定义的转换，我们支持了通过配置文件来映射两者之间的关系，进而来简化配置文件的开发。</p><p>在行为上表现为对任务描述Json文件中<code>reader</code>部分的<code>columns</code>的解析，对于<code>columns</code>中不同字段的type会根据上面描述文件从<code>ClickhouseReaderOptions.</code>*<code>COLUMNS</code>*字段中解析到<code>readerContext.getTypeInfos()</code>中。</p><h5 id="实现" tabindex="-1"><a class="header-anchor" href="#实现" aria-hidden="true">#</a> 实现</h5><ul><li><code>BitSailTypeInfoConverter</code><ul><li>默认的<code>TypeInfoConverter</code>，直接对<code>ReaderOptions.</code>*<code>COLUMNS</code><em>字段进行字符串的直接解析，</em><code>COLUMNS</code>*字段中是什么类型，<code>TypeInfoConverter</code>中就是什么类型。</li></ul></li><li><code>FileMappingTypeInfoConverter</code><ul><li>会在BitSail类型系统转换时去绑定<code>{readername}-type-converter.yaml</code>文件，做数据库字段类型和BitSail类型的映射。<code>ReaderOptions.</code>*<code>COLUMNS</code>*字段在通过这个映射文件转换后才会映射到<code>TypeInfoConverter</code>中。</li></ul></li></ul><h5 id="示例-1" tabindex="-1"><a class="header-anchor" href="#示例-1" aria-hidden="true">#</a> 示例</h5><h6 id="filemappingtypeinfoconverter" tabindex="-1"><a class="header-anchor" href="#filemappingtypeinfoconverter" aria-hidden="true">#</a> FileMappingTypeInfoConverter</h6><p>通过JDBC方式连接的数据库，包括MySql、Oracle、SqlServer、Kudu、ClickHouse等。这里数据源的特点是以<code>java.sql.ResultSet</code>的接口形式返回获取的数据，对于这类数据库，我们往往将<code>TypeInfoConverter</code>对象设计为<code>FileMappingTypeInfoConverter</code>，这个对象会在BitSail类型系统转换时去绑定<code>{readername}-type-converter.yaml</code>文件，做数据库字段类型和BitSail类型的映射。</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>@Override
public TypeInfoConverter createTypeInfoConverter() {
  return new FileMappingTypeInfoConverter(getReaderName());
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p>对于<code>{readername}-type-converter.yaml</code>文件的解析，以<code>clickhouse-type-converter.yaml</code>为例。</p><div class="language-Plain line-numbers-mode" data-ext="Plain"><pre class="language-Plain"><code># Clickhouse Type to BitSail Type
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
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p>这个文件起到的作用是进行job描述json文件中<code>reader</code>部分的<code>columns</code>的解析，对于<code>columns</code>中不同字段的type会根据上面描述文件从<code>ClickhouseReaderOptions.</code>*<code>COLUMNS</code>*字段中解析到<code>readerContext.getTypeInfos()</code>中。</p><div class="language-Json line-numbers-mode" data-ext="Json"><pre class="language-Json"><code>&quot;reader&quot;: {
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
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p><img src="`+a+`" alt="" loading="lazy"></p><p>这种方式不仅仅适用于数据库，也适用于所有需要在类型转换中需要引擎侧和BitSail侧进行类型映射的场景。</p><h6 id="bitsailtypeinfoconverter" tabindex="-1"><a class="header-anchor" href="#bitsailtypeinfoconverter" aria-hidden="true">#</a> BitSailTypeInfoConverter</h6><p>通常采用默认的方式进行类型转换，直接对<code>ReaderOptions.\`\`COLUMNS</code>字段进行字符串的直接解析。</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>@Override
public TypeInfoConverter createTypeInfoConverter() {
  return new BitSailTypeInfoConverter();
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p>以Hadoop为例：</p><div class="language-Json line-numbers-mode" data-ext="Json"><pre class="language-Json"><code>&quot;reader&quot;: {
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
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p><img src="`+d+`" alt="" loading="lazy"></p><h4 id="createsourcereader方法" tabindex="-1"><a class="header-anchor" href="#createsourcereader方法" aria-hidden="true">#</a> createSourceReader方法</h4><p>书写具体的数据读取逻辑，负责数据读取的组件，在接收到Split后会对其进行数据读取，然后将数据传输给下一个算子。</p><p>具体传入构造SourceReader的参数按需求决定，但是一定要保证所有参数可以序列化。如果不可序列化，将会在createJobGraph的时候出错。</p><h5 id="示例-2" tabindex="-1"><a class="header-anchor" href="#示例-2" aria-hidden="true">#</a> 示例</h5><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public SourceReader&lt;Row, RocketMQSplit&gt; createReader(SourceReader.Context readerContext) {
  return new RocketMQSourceReader(
      readerConfiguration,
      readerContext,
      getSourceBoundedness());
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h4 id="createsplitcoordinator方法" tabindex="-1"><a class="header-anchor" href="#createsplitcoordinator方法" aria-hidden="true">#</a> createSplitCoordinator方法</h4><p>书写具体的数据分片、分片分配逻辑，SplitCoordinator承担了去创建、管理Split的角色。</p><p>具体传入构造SplitCoordinator的参数按需求决定，但是一定要保证所有参数可以序列化。如果不可序列化，将会在createJobGraph的时候出错。</p><h5 id="示例-3" tabindex="-1"><a class="header-anchor" href="#示例-3" aria-hidden="true">#</a> 示例</h5><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public SourceSplitCoordinator&lt;RocketMQSplit, RocketMQState&gt; createSplitCoordinator(SourceSplitCoordinator
                                                                                       .Context&lt;RocketMQSplit, RocketMQState&gt; coordinatorContext) {
  return new RocketMQSourceSplitCoordinator(
      coordinatorContext,
      readerConfiguration,
      getSourceBoundedness());
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="parallelismcomputable接口" tabindex="-1"><a class="header-anchor" href="#parallelismcomputable接口" aria-hidden="true">#</a> ParallelismComputable接口</h3><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public interface ParallelismComputable extends Serializable {

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
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h4 id="getparallelismadvice方法" tabindex="-1"><a class="header-anchor" href="#getparallelismadvice方法" aria-hidden="true">#</a> getParallelismAdvice方法</h4><p>用于指定下游reader的并行数目。一般有以下的方式：</p><p>可以选择<code>selfConf.get(ClickhouseReaderOptions.</code><em><code>READER_PARALLELISM_NUM</code></em><code>)</code>来指定并行度。</p><p>也可以自定义自己的并行度划分逻辑。</p><h5 id="示例-4" tabindex="-1"><a class="header-anchor" href="#示例-4" aria-hidden="true">#</a> 示例</h5><p>比如在RocketMQ中，我们可以定义每1个reader可以处理至多4个队列*<code>DEFAULT_ROCKETMQ_PARALLELISM_THRESHOLD </code>*<code>= 4</code></p><p>通过这种自定义的方式获取对应的并行度。</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public ParallelismAdvice getParallelismAdvice(BitSailConfiguration commonConfiguration,
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
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h2 id="sourcesplit" tabindex="-1"><a class="header-anchor" href="#sourcesplit" aria-hidden="true">#</a> SourceSplit</h2><p>数据源的数据分片格式，需要我们实现SourceSplit接口。</p><p><img src="`+r+`" alt="" loading="lazy"></p><h3 id="sourcesplit接口" tabindex="-1"><a class="header-anchor" href="#sourcesplit接口" aria-hidden="true">#</a> SourceSplit接口</h3><p>要求我们实现一个实现一个获取splitId的方法。</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public interface SourceSplit extends Serializable {
  String uniqSplitId();
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p>对于具体切片的格式，开发者可以按照自己的需求进行自定义。</p><h3 id="示例-5" tabindex="-1"><a class="header-anchor" href="#示例-5" aria-hidden="true">#</a> 示例</h3><h4 id="jdbc类存储" tabindex="-1"><a class="header-anchor" href="#jdbc类存储" aria-hidden="true">#</a> JDBC类存储</h4><p>一般会通过主键，来对数据进行最大、最小值的划分；对于无主键类则通常会将其认定为一个split，不再进行拆分，所以split中的参数包括主键的最大最小值，以及一个布尔类型的<code>readTable</code>，如果无主键类或是不进行主键的切分则整张表会视为一个split，此时<code>readTable</code>为<code>true</code>，如果按主键最大最小值进行切分，则设置为<code>false</code>。</p><p>以ClickhouseSourceSplit为例：</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>@Setter
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
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h4 id="消息队列" tabindex="-1"><a class="header-anchor" href="#消息队列" aria-hidden="true">#</a> 消息队列</h4><p>一般按照消息队列中topic注册的partitions的数量进行split的划分，切片中主要应包含消费的起点和终点以及消费的队列。</p><p>以RocketMQSplit为例：</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>@Builder
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
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h4 id="文件系统" tabindex="-1"><a class="header-anchor" href="#文件系统" aria-hidden="true">#</a> 文件系统</h4><p>一般会按照文件作为最小粒度进行划分，同时有些格式也支持将单个文件拆分为多个子Splits。文件系统split中需要包装所需的文件切片。</p><p>以FtpSourceSplit为例：</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public class FtpSourceSplit implements SourceSplit {

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
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p>特别的，在Hadoop文件系统中，我们也可以利用对<code>org.apache.hadoop.mapred.InputSplit</code>类的包装来自定义我们的Split。</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public class HadoopSourceSplit implements SourceSplit {
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
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h2 id="state" tabindex="-1"><a class="header-anchor" href="#state" aria-hidden="true">#</a> State</h2><p>在需要做checkpoint的场景下，通常我们会通过Map来保留当前的执行状态</p><h3 id="流批一体场景" tabindex="-1"><a class="header-anchor" href="#流批一体场景" aria-hidden="true">#</a> 流批一体场景</h3><p>在流批一体场景中，我们需要保存状态以便从异常中断的流式作业恢复</p><p>以RocketMQState为例：</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public class RocketMQState implements Serializable {

  private final Map&lt;MessageQueue, String&gt; assignedWithSplitIds;

  public RocketMQState(Map&lt;MessageQueue, String&gt; assignedWithSplitIds) {
    this.assignedWithSplitIds = assignedWithSplitIds;
  }

  public Map&lt;MessageQueue, String&gt; getAssignedWithSplits() {
    return assignedWithSplitIds;
  }
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="批式场景" tabindex="-1"><a class="header-anchor" href="#批式场景" aria-hidden="true">#</a> 批式场景</h3><p>对于批式场景，我们可以使用<code>EmptyState</code>不存储状态，如果需要状态存储，和流批一体场景采用相似的设计方案。</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public class EmptyState implements Serializable {

  public static EmptyState fromBytes() {
    return new EmptyState();
  }
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h2 id="sourcesplitcoordinator" tabindex="-1"><a class="header-anchor" href="#sourcesplitcoordinator" aria-hidden="true">#</a> SourceSplitCoordinator</h2><p>大数据处理框架的核心目的就是将大规模的数据拆分成为多个合理的Split，SplitCoordinator承担这个创建、管理Split的角色。</p><p><img src="`+t+`" alt="" loading="lazy"></p><h3 id="sourcesplitcoordinator接口" tabindex="-1"><a class="header-anchor" href="#sourcesplitcoordinator接口" aria-hidden="true">#</a> SourceSplitCoordinator接口</h3><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public interface SourceSplitCoordinator&lt;SplitT extends SourceSplit, StateT&gt; extends Serializable, AutoCloseable {

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
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="构造方法" tabindex="-1"><a class="header-anchor" href="#构造方法" aria-hidden="true">#</a> 构造方法</h3><p>开发者在构造方法中一般主要进行一些配置的设置和分片信息存储的容器的创建。</p><p>以ClickhouseSourceSplitCoordinator的构造为例：</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public ClickhouseSourceSplitCoordinator(SourceSplitCoordinator.Context&lt;ClickhouseSourceSplit, EmptyState&gt; context,
                                  BitSailConfiguration jobConf) {
  this.context = context;
  this.jobConf = jobConf;
  this.splitAssignmentPlan = Maps.newConcurrentMap();
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p>在自定义了State的场景中，需要对checkpoint时存储在<code>SourceSplitCoordinator.Context</code>的状态进行保存和恢复。</p><p>以RocketMQSourceSplitCoordinator为例：</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public RocketMQSourceSplitCoordinator(
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
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="start方法" tabindex="-1"><a class="header-anchor" href="#start方法" aria-hidden="true">#</a> start方法</h3><p>进行一些数据源所需分片元数据的提取工作，如果有抽象出来的Split Assigner类，一般在这里进行初始化。如果使用的是封装的Split Assign函数，这里会进行待分配切片的初始化工作。</p><h4 id="流批一体场景-1" tabindex="-1"><a class="header-anchor" href="#流批一体场景-1" aria-hidden="true">#</a> 流批一体场景</h4><p>以RocketMQSourceSplitCoordinator为例：</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>private void prepareRocketMQConsumer() {
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
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h4 id="批式场景-1" tabindex="-1"><a class="header-anchor" href="#批式场景-1" aria-hidden="true">#</a> 批式场景</h4><p>以ClickhouseSourceSplitCoordinator为例：</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public void start() {
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
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="assigner" tabindex="-1"><a class="header-anchor" href="#assigner" aria-hidden="true">#</a> Assigner</h3><p>将划分好的切片分配给Reader，开发过程中，我们通常让SourceSplitCoordinator专注于处理和Reader 的通讯工作，实际split的分发逻辑一般封装在Assigner进行，这个Assigner可以是一个封装的Split Assign函数，也可以是一个抽象出来的Split Assigner类。</p><h4 id="assign函数示例" tabindex="-1"><a class="header-anchor" href="#assign函数示例" aria-hidden="true">#</a> Assign函数示例</h4><p>以ClickhouseSourceSplitCoordinator为例：</p><p>tryAssignSplitsToReader函数将存储在splitAssignmentPlan中的划分好的切片分配给相应的Reader。</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>private void tryAssignSplitsToReader() {
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
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h4 id="assigner方法示例" tabindex="-1"><a class="header-anchor" href="#assigner方法示例" aria-hidden="true">#</a> Assigner方法示例</h4><p>以RocketMQSourceSplitCoordinator为例：</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public class FairRocketMQSplitAssigner implements SplitAssigner&lt;MessageQueue&gt; {

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
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="addreader方法" tabindex="-1"><a class="header-anchor" href="#addreader方法" aria-hidden="true">#</a> addReader方法</h3><p>调用Assigner，为Reader添加切片。</p><h4 id="批式场景示例-1" tabindex="-1"><a class="header-anchor" href="#批式场景示例-1" aria-hidden="true">#</a> 批式场景示例</h4><p>以ClickhouseSourceSplitCoordinator为例：</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public void addReader(int subtaskId) {
  LOG.info(&quot;Found reader {}&quot;, subtaskId);
  tryAssignSplitsToReader();
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h4 id="流批一体场景示例-1" tabindex="-1"><a class="header-anchor" href="#流批一体场景示例-1" aria-hidden="true">#</a> 流批一体场景示例</h4><p>以RocketMQSourceSplitCoordinator为例：</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>private void notifyReaderAssignmentResult() {
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
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="addsplitsback方法" tabindex="-1"><a class="header-anchor" href="#addsplitsback方法" aria-hidden="true">#</a> addSplitsBack方法</h3><p>对于一些Reader没有处理完的切片，进行重新分配，重新分配的策略可以自己定义，常用的策略是哈希取模，对于返回的Split列表中的所有Split进行重新分配后再Assign给不同的Reader。</p><h4 id="批式场景示例-2" tabindex="-1"><a class="header-anchor" href="#批式场景示例-2" aria-hidden="true">#</a> 批式场景示例</h4><p>以ClickhouseSourceSplitCoordinator为例：</p><p>ReaderSelector使用哈希取模的策略对Split列表进行重分配。</p><p>tryAssignSplitsToReader方法将重分配后的Split集合通过Assigner分配给Reader。</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public void addSplitsBack(List&lt;ClickhouseSourceSplit&gt; splits, int subtaskId) {
  LOG.info(&quot;Source reader {} return splits {}.&quot;, subtaskId, splits);

  int readerNum = context.totalParallelism();
  for (ClickhouseSourceSplit split : splits) {
    int readerIndex = ReaderSelector.getReaderIndex(readerNum);
    splitAssignmentPlan.computeIfAbsent(readerIndex, k -&gt; new HashSet&lt;&gt;()).add(split);
    LOG.info(&quot;Re-assign split {} to the {}-th reader.&quot;, split.uniqSplitId(), readerIndex);
  }

  tryAssignSplitsToReader();
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h4 id="流批一体场景示例-2" tabindex="-1"><a class="header-anchor" href="#流批一体场景示例-2" aria-hidden="true">#</a> 流批一体场景示例</h4><p>以RocketMQSourceSplitCoordinator为例：</p><p>addSplitChangeToPendingAssignment使用哈希取模的策略对Split列表进行重分配。</p><p>notifyReaderAssignmentResult将重分配后的Split集合通过Assigner分配给Reader。</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>private synchronized void addSplitChangeToPendingAssignment(Set&lt;RocketMQSplit&gt; newRocketMQSplits) {
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
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="snapshotstate方法" tabindex="-1"><a class="header-anchor" href="#snapshotstate方法" aria-hidden="true">#</a> snapshotState方法</h3><p>存储处理切片的快照信息，用于恢复时在构造方法中使用。</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public RocketMQState snapshotState() throws Exception {
  return new RocketMQState(assignedPartitions);
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="close方法" tabindex="-1"><a class="header-anchor" href="#close方法" aria-hidden="true">#</a> close方法</h3><p>关闭在分片过程中与数据源交互读取元数据信息的所有未关闭连接器。</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public void close() {
  if (consumer != null) {
    consumer.shutdown();
  }
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h2 id="sourcereader" tabindex="-1"><a class="header-anchor" href="#sourcereader" aria-hidden="true">#</a> SourceReader</h2><p>每个SourceReader都在独立的线程中执行，只要我们保证SourceSplitCoordinator分配给不同SourceReader的切片没有交集，在SourceReader的执行周期中，我们就可以不考虑任何有关并发的细节。</p><p><img src="`+v+`" alt="" loading="lazy"></p><h3 id="sourcereader接口" tabindex="-1"><a class="header-anchor" href="#sourcereader接口" aria-hidden="true">#</a> SourceReader接口</h3><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public interface SourceReader&lt;T, SplitT extends SourceSplit&gt; extends Serializable, AutoCloseable {

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
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="构造方法-1" tabindex="-1"><a class="header-anchor" href="#构造方法-1" aria-hidden="true">#</a> 构造方法</h3><p>这里需要完成和数据源访问各种配置的提取，比如数据库库名表名、消息队列cluster和topic、身份认证的配置等等。</p><h4 id="示例-6" tabindex="-1"><a class="header-anchor" href="#示例-6" aria-hidden="true">#</a> 示例</h4><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public RocketMQSourceReader(BitSailConfiguration readerConfiguration,
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
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="start方法-1" tabindex="-1"><a class="header-anchor" href="#start方法-1" aria-hidden="true">#</a> start方法</h3><p>初始化数据源的访问对象，例如数据库的执行对象、消息队列的consumer对象或者文件系统的连接。</p><h4 id="示例-7" tabindex="-1"><a class="header-anchor" href="#示例-7" aria-hidden="true">#</a> 示例</h4><p>消息队列</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public void start() {
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
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p>数据库</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public void start() {
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
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="addsplits方法" tabindex="-1"><a class="header-anchor" href="#addsplits方法" aria-hidden="true">#</a> addSplits方法</h3><p>将SourceSplitCoordinator给当前Reader分配的Splits列表添加到自己的处理队列（Queue）或者集合（Set）中。</p><h4 id="示例-8" tabindex="-1"><a class="header-anchor" href="#示例-8" aria-hidden="true">#</a> 示例</h4><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public void addSplits(List&lt;RocketMQSplit&gt; splits) {
  LOG.info(&quot;Subtask {} received {}(s) new splits, splits = {}.&quot;,
      context.getIndexOfSubtask(),
      CollectionUtils.size(splits),
      splits);

  assignedRocketMQSplits.addAll(splits);
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="hasmoreelements方法" tabindex="-1"><a class="header-anchor" href="#hasmoreelements方法" aria-hidden="true">#</a> hasMoreElements方法</h3><p>在无界的流计算场景中，会一直返回true保证Reader线程不被销毁。</p><p>在批式场景中，分配给该Reader的切片处理完之后会返回false，表示该Reader生命周期的结束。</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public boolean hasMoreElements() {
  if (boundedness == Boundedness.UNBOUNDEDNESS) {
    return true;
  }
  if (noMoreSplits) {
    return CollectionUtils.size(assignedRocketMQSplits) != 0;
  }
  return true;
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="pollnext方法" tabindex="-1"><a class="header-anchor" href="#pollnext方法" aria-hidden="true">#</a> pollNext方法</h3><p>在addSplits方法添加完成切片处理队列且hasMoreElements返回true时，该方法调用，开发者实现此方法真正和数据交互。</p><p>开发者在实现pollNext方法时候需要关注下列问题：</p><ul><li>切片数据的读取 <ul><li>从构造好的切片中去读取数据。</li></ul></li><li>数据类型的转换 <ul><li>将外部数据转换成BitSail的Row类型</li></ul></li></ul><h4 id="示例-9" tabindex="-1"><a class="header-anchor" href="#示例-9" aria-hidden="true">#</a> 示例</h4><p>以RocketMQSourceReader为例：</p><p>从split队列中选取split进行处理，读取其信息，之后需要将读取到的信息转换成BitSail的Row类型，发送给下游处理。</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public void pollNext(SourcePipeline&lt;Row&gt; pipeline) throws Exception {
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
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h4 id="转换为bitsail-row类型的常用方式" tabindex="-1"><a class="header-anchor" href="#转换为bitsail-row类型的常用方式" aria-hidden="true">#</a> 转换为BitSail Row类型的常用方式</h4><h5 id="自定义rowdeserializer类" tabindex="-1"><a class="header-anchor" href="#自定义rowdeserializer类" aria-hidden="true">#</a> 自定义RowDeserializer类</h5><p>对于不同格式的列应用不同converter，设置到相应Row的Field。</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public class ClickhouseRowDeserializer {

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
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h5 id="实现deserializationschema接口" tabindex="-1"><a class="header-anchor" href="#实现deserializationschema接口" aria-hidden="true">#</a> 实现DeserializationSchema接口</h5><p>相对于实现RowDeserializer，我们更希望大家去实现一个继承DeserializationSchema接口的实现类，将一定类型格式的数据对数据比如JSON、CSV转换为BitSail Row类型。</p><p><img src="`+u+`" alt="" loading="lazy"></p><p>在具体的应用时，我们可以使用统一的接口创建相应的实现类</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public class TextInputFormatDeserializationSchema implements DeserializationSchema&lt;Writable, Row&gt; {

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
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p>也可以自定义当前需要解析类专用的DeserializationSchema：</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public class MapredParquetInputFormatDeserializationSchema implements DeserializationSchema&lt;Writable, Row&gt; {

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
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="snapshotstate方法-1" tabindex="-1"><a class="header-anchor" href="#snapshotstate方法-1" aria-hidden="true">#</a> snapshotState方法</h3><p>生成并保存State的快照信息，用于ckeckpoint。</p><h4 id="示例-10" tabindex="-1"><a class="header-anchor" href="#示例-10" aria-hidden="true">#</a> 示例</h4><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public List&lt;RocketMQSplit&gt; snapshotState(long checkpointId) {
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
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="hasmoreelements方法-1" tabindex="-1"><a class="header-anchor" href="#hasmoreelements方法-1" aria-hidden="true">#</a> hasMoreElements方法</h3><p>每次调用pollNext方法之前会做sourceReader.hasMoreElements()的判断，当且仅当判断通过，pollNext方法才会被调用。</p><h4 id="示例-11" tabindex="-1"><a class="header-anchor" href="#示例-11" aria-hidden="true">#</a> 示例</h4><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public boolean hasMoreElements() {
  if (noMoreSplits) {
    return CollectionUtils.size(assignedHadoopSplits) != 0;
  }
  return true;
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="notifynomoresplits方法" tabindex="-1"><a class="header-anchor" href="#notifynomoresplits方法" aria-hidden="true">#</a> notifyNoMoreSplits方法</h3><p>当Reader处理完所有切片之后，会调用此方法。</p><h4 id="示例-12" tabindex="-1"><a class="header-anchor" href="#示例-12" aria-hidden="true">#</a> 示例</h4><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public void notifyNoMoreSplits() {
  LOG.info(&quot;Subtask {} received no more split signal.&quot;, context.getIndexOfSubtask());
  noMoreSplits = true;
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div>`,206);function I(y,R){const n=h("RouterLink");return o(),m("div",null,[f,e("p",null,[b(n,{to:"/en/community/source_connector_detail.html"},{default:p(()=>[i("English")]),_:1}),i(" | 简体中文")]),C])}const E=c(S,[["render",I],["__file","source_connector_detail.html.vue"]]);export{E as default};
