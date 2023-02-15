import{_ as r,a as d,b as s,c as l}from"./test_container.21189e4b.js";import{_ as c}from"./bitsail_model.b409088b.js";import{_ as u}from"./sink_connector.3fde728e.js";import{_ as h}from"./_plugin-vue_export-helper.cdc0426e.js";import{o as m,c as v,a as e,b as t,d as i,w as p,e as b,r as o}from"./app.adeb8394.js";const f="/bitsail/assets/code_structure_en.c7fe3114.png",g={},q=e("h1",{id:"connector-quick-start",tabindex:"-1"},[e("a",{class:"header-anchor",href:"#connector-quick-start","aria-hidden":"true"},"#"),t(" Connector Quick Start")],-1),y=e("hr",null,null,-1),S=e("h2",{id:"introduction",tabindex:"-1"},[e("a",{class:"header-anchor",href:"#introduction","aria-hidden":"true"},"#"),t(" Introduction")],-1),w=e("p",null,"This article is aimed at BitSail's Connector developers. It comprehensively explains the whole process of developing a complete Connector from the developer's perspective, and quickly gets started with Connector development.",-1),x=e("h2",{id:"contents",tabindex:"-1"},[e("a",{class:"header-anchor",href:"#contents","aria-hidden":"true"},"#"),t(" contents")],-1),_={href:"https://docs.github.com/en/get-started/quickstart/fork-a-repo",target:"_blank",rel:"noopener noreferrer"},k={href:"https://github.com/bytedance/bitsail.git",target:"_blank",rel:"noopener noreferrer"},C=b('<p>The project structure is as follows:</p><p><img src="'+f+'" alt="" loading="lazy"></p><h2 id="development-process" tabindex="-1"><a class="header-anchor" href="#development-process" aria-hidden="true">#</a> Development Process</h2><p>BitSail is a data integration engine based on a distributed architecture, and Connectors will execute concurrently. And the BitSail framework is responsible for task scheduling, concurrent execution, dirty data processing, etc. Developers only need to implement the corresponding interface. The specific development process is as follows:</p><ul><li>Project configuration, developers need to register their own Connector in the <code>bitsail/bitsail-connectors/pom.xml</code> module, add their own Connector module in <code>bitsail/bitsail-dist/pom.xml</code>, and register configuration files for your connector , so that the framework can dynamically discover it at runtime <ul><li><p><img src="'+r+'" alt="" loading="lazy"></p></li><li><p><img src="'+d+'" alt="" loading="lazy"></p></li></ul></li><li>Connector development, implement the abstract methods provided by Source and Sink, refer to the follow-up introduction for details</li><li>Data output type, the currently supported data type is the BitSail Row type, whether it is the data type that the Source passes to the downstream in the Reader, or the data type that the Sink consumes from the upstream, it should be the BitSail Row type</li></ul><h1 id="architecture" tabindex="-1"><a class="header-anchor" href="#architecture" aria-hidden="true">#</a> Architecture</h1><p>The current design of the Source API is also compatible with streaming and batch scenarios, in other words, it supports pull &amp; push scenarios at the same time. Before that, we need to go through the interaction model of each component in the traditional streaming batch scenario.</p><h2 id="batch-model" tabindex="-1"><a class="header-anchor" href="#batch-model" aria-hidden="true">#</a> Batch Model</h2><p>In traditional batch scenarios, data reading is generally divided into the following steps:</p><ul><li><code>createSplits</code>：It is generally executed on the client side or the central node. The purpose is to split the complete data into as many <code>rangeSplits</code> as possible according to the specified rules. <code>createSplits</code> is executed once in the job life cycle.</li><li><code>runWithSplit</code>: Generally, it is executed on the execution node. After the execution node is started, it will request the existing <code>rangeSplit</code> from the central node, and then execute it locally; after the execution is completed, it will request the central node again until all the <code>splits </code>are executed.</li><li><code>commit</code>：After the execution of all splits is completed, the <code>commit</code> operation is generally performed on the central node to make the data visible to the outside world.</li></ul><h2 id="stream-model" tabindex="-1"><a class="header-anchor" href="#stream-model" aria-hidden="true">#</a> Stream Model</h2><p>In traditional streaming scenarios, data reading is generally divided into the following steps:</p><ul><li><code>createSplits</code>: generally executed on the client side or the central node, the purpose is to divide the data stream into <code>rangeSplits</code> according to the sliding window or tumbling window strategy, and <code>createSplits</code> will always be executed according to the divided windows during the life cycle of the streaming job.</li><li><code>runWithSplit</code>: Generally executed on the execution node, the central node will send <code>rangeSplit</code> to the executable node, and then execute locally on the executable node; after the execution is completed, the processed <code>splits</code> data will be sent downstream.</li><li><code>commit</code>: After the execution of all splits is completed, the <code>retract message</code> is generally sent to the target data source, and the results are dynamically displayed in real time.</li></ul><h2 id="bitsail-model" tabindex="-1"><a class="header-anchor" href="#bitsail-model" aria-hidden="true">#</a> BitSail Model</h2><p><img src="'+c+'" alt="" loading="lazy"></p><ul><li><code>createSplits</code>: BitSail divides rangeSplits through the <code>SplitCoordinator</code> module. <code>createSplits</code> will be executed periodically in the life cycle of streaming jobs, but only once in batch jobs.</li><li><code>runWithSplit</code>: Execute on the execution node. The execution node in BitSail includes <code>Reader</code> and <code>Writer</code> modules. The central node will send <code>rangeSplit</code> to the executable node, and then execute locally on the executable node; after the execution is completed, the processed <code>splits</code> data will be sent downstream.</li><li><code>commit</code>: After the <code>writer</code> completes the data writing, the <code>committer</code> completes the submission. When <code>checkpoint</code> is not enabled, <code>commit</code> will be executed once after all <code>writers</code> are finished; when <code>checkpoint</code> is enabled, <code>commit</code> will be executed once every checkpoint.</li></ul><h1 id="source-connector" tabindex="-1"><a class="header-anchor" href="#source-connector" aria-hidden="true">#</a> Source Connector</h1><h2 id="introduction-1" tabindex="-1"><a class="header-anchor" href="#introduction-1" aria-hidden="true">#</a> Introduction</h2><p><img src="'+s+`" alt="" loading="lazy"></p><ul><li>Source: The life cycle management component of the data reading component is mainly responsible for interacting with the framework, structuring the job, and not participating in the actual execution of the job.</li><li>SourceSplit: Source data split. The core purpose of the big data processing framework is to split large-scale data into multiple reasonable Splits</li><li>State：Job status snapshot. When the checkpoint is enabled, the current execution status will be saved.</li><li>SplitCoordinator: SplitCoordinator assumes the role of creating and managing Split.</li><li>SourceReader: The component that is actually responsible for data reading will read the data after receiving the Split, and then transmit the data to the next operator.</li></ul><p>Developers first need to create the <code>Source</code> class, which needs to implement the <code>Source</code> and <code>ParallelismComputable</code> interfaces. It is mainly responsible for interacting with the framework and structuring the job. It does not participate in the actual execution of the job. BitSail&#39;s <code>Source</code> adopts the design idea of stream-batch integration, sets the job processing method through the <code>getSourceBoundedness</code> method, defines the <code>readerConfiguration</code> through the <code>configure</code> method, and performs data type conversion through the <code>createTypeInfoConverter</code> method, and can obtain user-defined data in the yaml file through <code>FileMappingTypeInfoConverter</code> The conversion between source type and BitSail type realizes customized type conversion. Then we define the data fragmentation format <code>SourceSplit</code> class of the data source and the <code>SourceSplitCoordinator</code> class that will manage the Split role, and finally complete the <code>SourceReader</code> to read data from the Split.</p><table><thead><tr><th>Job Type</th><th>Boundedness</th></tr></thead><tbody><tr><td>batch</td><td>Boundedness.<em>BOUNDEDNESS</em></td></tr><tr><td>stream</td><td>Boundedness.<em>UNBOUNDEDNESS</em></td></tr></tbody></table><p>Each <code>SourceReader</code> is executed in an independent thread. As long as we ensure that the splits assigned by the <code>SourceSplitCoordinator</code> to different <code>SourceReader</code> do not overlap, the developer doesn&#39;t consider any concurrency details during the execution cycle of <code>SourceReader</code>. You only need to pay attention to how to read data from the constructed split, then complete the data type conversion, and convert the external data type into BitSail’s Row type and pass it downstream.</p><h2 id="reader-example" tabindex="-1"><a class="header-anchor" href="#reader-example" aria-hidden="true">#</a> Reader example</h2><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public class FakeSourceReader extends SimpleSourceReaderBase&lt;Row&gt; {

  private final BitSailConfiguration readerConfiguration;
  private final TypeInfo&lt;?&gt;[] typeInfos;

  private final transient int totalCount;
  private final transient RateLimiter fakeGenerateRate;
  private final transient AtomicLong counter;

  private final FakeRowGenerator fakeRowGenerator;

  public FakeSourceReader(BitSailConfiguration readerConfiguration, Context context) {
    this.readerConfiguration = readerConfiguration;
    this.typeInfos = context.getTypeInfos();
    this.totalCount = readerConfiguration.get(FakeReaderOptions.TOTAL_COUNT);
    this.fakeGenerateRate = RateLimiter.create(readerConfiguration.get(FakeReaderOptions.RATE));
    this.counter = new AtomicLong();
    this.fakeRowGenerator = new FakeRowGenerator(readerConfiguration, context.getIndexOfSubtask());
  }

  @Override
  public void pollNext(SourcePipeline&lt;Row&gt; pipeline) throws Exception {
    fakeGenerateRate.acquire();
    pipeline.output(fakeRowGenerator.fakeOneRecord(typeInfos));
  }

  @Override
  public boolean hasMoreElements() {
    return counter.incrementAndGet() &lt;= totalCount;
  }
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h1 id="sink-connector" tabindex="-1"><a class="header-anchor" href="#sink-connector" aria-hidden="true">#</a> Sink Connector</h1><h2 id="introduction-2" tabindex="-1"><a class="header-anchor" href="#introduction-2" aria-hidden="true">#</a> Introduction</h2><p><img src="`+u+`" alt="" loading="lazy"></p><ul><li>Sink: life cycle management of data writing components, mainly responsible for interaction with the framework, framing jobs, it does not participate in the actual execution of jobs.</li><li>Writer: responsible for writing the received data to external storage.</li><li>WriterCommitter (optional): Commit the data to complete the two-phase commit operation; realize the semantics of exactly-once.</li></ul><p>Developers first need to create a <code>Sink</code> class and implement the <code>Sink</code> interface, which is mainly responsible for the life cycle management of the data writing component and the construction of the job. Define the configuration of <code>writerConfiguration</code> through the <code>configure</code> method, perform data type conversion through the <code>createTypeInfoConverter</code> method, and write the internal type conversion to the external system, the same as the <code>Source</code> part. Then we define the <code>Writer</code> class to implement the specific data writing logic. When the <code>write</code> method is called, the BitSail Row type writes the data into the cache queue, and when the <code>flush</code> method is called, the data in the cache queue is flushed to the target data source.</p><h2 id="writer-example" tabindex="-1"><a class="header-anchor" href="#writer-example" aria-hidden="true">#</a> Writer example</h2><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public class PrintWriter implements Writer&lt;Row, String, Integer&gt; {
  private static final Logger LOG = LoggerFactory.getLogger(PrintWriter.class);

  private final int batchSize;
  private final List&lt;String&gt; fieldNames;

  private final List&lt;String&gt; writeBuffer;
  private final List&lt;String&gt; commitBuffer;

  private final AtomicInteger printCount;

  public PrintWriter(int batchSize, List&lt;String&gt; fieldNames) {
    this(batchSize, fieldNames, 0);
  }

  public PrintWriter(int batchSize, List&lt;String&gt; fieldNames, int alreadyPrintCount) {
    Preconditions.checkState(batchSize &gt; 0, &quot;batch size must be larger than 0&quot;);
    this.batchSize = batchSize;
    this.fieldNames = fieldNames;
    this.writeBuffer = new ArrayList&lt;&gt;(batchSize);
    this.commitBuffer = new ArrayList&lt;&gt;(batchSize);
    printCount = new AtomicInteger(alreadyPrintCount);
  }

  @Override
  public void write(Row element) {
    String[] fields = new String[element.getFields().length];
    for (int i = 0; i &lt; element.getFields().length; ++i) {
      fields[i] = String.format(&quot;\\&quot;%s\\&quot;:\\&quot;%s\\&quot;&quot;, fieldNames.get(i), element.getField(i).toString());
    }

    writeBuffer.add(&quot;[&quot; + String.join(&quot;,&quot;, fields) + &quot;]&quot;);
    if (writeBuffer.size() == batchSize) {
      this.flush(false);
    }
    printCount.incrementAndGet();
  }

  @Override
  public void flush(boolean endOfInput) {
    commitBuffer.addAll(writeBuffer);
    writeBuffer.clear();
    if (endOfInput) {
      LOG.info(&quot;all records are sent to commit buffer.&quot;);
    }
  }

  @Override
  public List&lt;String&gt; prepareCommit() {
    return commitBuffer;
  }

  @Override
  public List&lt;Integer&gt; snapshotState(long checkpointId) {
    return Collections.singletonList(printCount.get());
  }
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h1 id="register-the-connector-into-the-configuration-file" tabindex="-1"><a class="header-anchor" href="#register-the-connector-into-the-configuration-file" aria-hidden="true">#</a> Register the connector into the configuration file</h1><p>Register a configuration file for your connector so that the framework can dynamically discover it at runtime. The configuration file is defined as follows:</p><p>Taking hive as an example, developers need to add a json file in the resource directory. The example name is <code>bitsail-connector-hive.json</code>, as long as it does not overlap with other connectors.</p><div class="language-Json line-numbers-mode" data-ext="Json"><pre class="language-Json"><code>{
  &quot;name&quot;: &quot;bitsail-connector-hive&quot;,
  &quot;classes&quot;: [
    &quot;com.bytedance.bitsail.connector.hive.source.HiveSource&quot;,
    &quot;com.bytedance.bitsail.connector.hive.sink.HiveSink&quot;
  ],
  &quot;libs&quot;: [
    &quot;bitsail-connector-hive-\${version}.jar&quot;
  ]
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h1 id="test-module" tabindex="-1"><a class="header-anchor" href="#test-module" aria-hidden="true">#</a> Test module</h1><p>In the module where the Source or Sink connector is located, add an ITCase test case, and then support it according to the following process.</p><ul><li>Start data source through the test container</li></ul><p><img src="`+l+`" alt="" loading="lazy"></p><ul><li>Write the corresponding configuration file</li></ul><div class="language-Json line-numbers-mode" data-ext="Json"><pre class="language-Json"><code>{
  &quot;job&quot;: {
    &quot;common&quot;: {
      &quot;job_id&quot;: 313,
      &quot;instance_id&quot;: 3123,
      &quot;job_name&quot;: &quot;bitsail_clickhouse_to_print_test&quot;,
      &quot;user_name&quot;: &quot;test&quot;
    },
    &quot;reader&quot;: {
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
    &quot;writer&quot;: {
      &quot;class&quot;: &quot;com.bytedance.bitsail.connector.legacy.print.sink.PrintSink&quot;
    }
  }
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><ul><li>Submit the job through EmbeddedFlinkCluster.submit method</li></ul><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>@Test
public void testClickhouseToPrint() throws Exception {
  BitSailConfiguration jobConf = JobConfUtils.fromClasspath(&quot;clickhouse_to_print.json&quot;);
  EmbeddedFlinkCluster.submitJob(jobConf);
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h1 id="submit-your-pr" tabindex="-1"><a class="header-anchor" href="#submit-your-pr" aria-hidden="true">#</a> Submit your PR</h1><p>After the developer implements his own Connector, he can associate his own issue and submit a PR to github. Before submitting, the developer remembers to add documents to the Connector. After passing the review, the Connector contributed by everyone becomes a part of BitSail. We follow the level of contribution and select active Contributors to become our Committers and participate in major decisions in the BitSail community. We hope that everyone will actively participate!</p>`,46);function B(R,I){const a=o("RouterLink"),n=o("ExternalLinkIcon");return m(),v("div",null,[q,e("p",null,[t("English | "),i(a,{to:"/zh/community/connector_quick_start.html"},{default:p(()=>[t("简体中文")]),_:1})]),y,S,w,x,e("p",null,[t("First, the developer needs to fork the BitSail repository. For more details, refer to "),e("a",_,[t("Fork BitSail Repo"),i(n)]),t(". And then use git clone the repository to the local, and import it into the IDE. At the same time, create your own working branch and use this branch to develop your own Connector. project address: "),e("a",k,[t("https://github.com/bytedance/bitsail.git"),i(n)]),t(".")]),C])}const J=h(g,[["render",B],["__file","connector_quick_start.html.vue"]]);export{J as default};
