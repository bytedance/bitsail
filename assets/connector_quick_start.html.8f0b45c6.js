import{_ as l,a as r,b as a,c as s}from"./test_container.21189e4b.js";import{_ as c}from"./bitsail_model.b409088b.js";import{_ as u}from"./sink_connector.3fde728e.js";import{_ as v}from"./_plugin-vue_export-helper.cdc0426e.js";import{o as m,c as b,a as e,d as n,w as p,b as i,e as h,r as d}from"./app.416da474.js";const q="/bitsail/assets/code_structure_zh.1912a4de.png",f={},g=e("h1",{id:"connector开发指南",tabindex:"-1"},[e("a",{class:"header-anchor",href:"#connector开发指南","aria-hidden":"true"},"#"),i(" Connector开发指南")],-1),S=e("hr",null,null,-1),_=e("h2",{id:"简介",tabindex:"-1"},[e("a",{class:"header-anchor",href:"#简介","aria-hidden":"true"},"#"),i(" 简介")],-1),C=e("p",null,"本文面向BitSail的Connector开发人员，通过开发者的角度全面的阐述开发一个完整Connector的全流程，快速上手Connector开发。",-1),k=e("h2",{id:"目录结构",tabindex:"-1"},[e("a",{class:"header-anchor",href:"#目录结构","aria-hidden":"true"},"#"),i(" 目录结构")],-1),y={href:"https://docs.github.com/en/get-started/quickstart/fork-a-repo",target:"_blank",rel:"noopener noreferrer"},B={href:"https://github.com/bytedance/bitsail.git%E3%80%82",target:"_blank",rel:"noopener noreferrer"},w=h('<p>项目结构如下：</p><p><img src="'+q+'" alt="" loading="lazy"></p><h2 id="开发流程" tabindex="-1"><a class="header-anchor" href="#开发流程" aria-hidden="true">#</a> 开发流程</h2><p>BitSail 是一款基于分布式架构的数据集成引擎，Connector会并发执行。并由BitSail 框架来负责任务的调度、并发执行、脏数据处理等，开发者只需要实现对应接口即可，具体开发流程如下：</p><ul><li>工程配置，开发者需要在<code>bitsail/bitsail-connectors/pom.xml</code>模块中注册自己的Connector，同时在<code>bitsail/bitsail-dist/pom.xml</code>增加自己的Connector模块，同时为你的连接器注册配置文件，来使得框架可以在运行时动态发现它。 <ul><li><p><img src="'+l+'" alt="" loading="lazy"></p></li><li><p><img src="'+r+'" alt="" loading="lazy"></p></li></ul></li><li>Connector开发，实现Source、Sink提供的抽象方法，具体细节参考后续介绍。</li><li>数据输出类型，目前支持的数据类型为BitSail Row类型，无论是Source在Reader中传递给下游的数据类型，还是Sink从上游消费的数据类型，都应该是BitSail Row类型。</li></ul><h2 id="architecture" tabindex="-1"><a class="header-anchor" href="#architecture" aria-hidden="true">#</a> Architecture</h2><p>当前Source API的设计同时兼容了流批一批的场景，换言之就是同时支持pull &amp; push 的场景。在此之前，我们需要首先再过一遍传统流批场景中各组件的交互模型。</p><h3 id="batch-model" tabindex="-1"><a class="header-anchor" href="#batch-model" aria-hidden="true">#</a> Batch Model</h3><p>传统批式场景中，数据的读取一般分为如下几步:</p><ul><li><code>createSplits</code>：一般在client端或者中心节点执行，目的是将完整的数据按照指定的规则尽可能拆分为较多的<code>rangeSplits</code>，<code>createSplits</code>在作业生命周期内有且执行一次。</li><li><code>runWithSplit</code>: 一般在执行节点节点执行，执行节点启动后会向中心节点请求存在的<code>rangeSplit</code>，然后再本地进行执行；执行完成后会再次向中心节点请求直到所有<code>splits</code>执行完成。</li><li><code>commit</code>：全部的split的执行完成后，一般会在中心节点执行<code>commit</code>的操作，用于将数据对外可见。</li></ul><h3 id="stream-model" tabindex="-1"><a class="header-anchor" href="#stream-model" aria-hidden="true">#</a> Stream Model</h3><p>传统流式场景中，数据的读取一般分为如下几步:</p><ul><li><code>createSplits</code>：一般在client端或者中心节点执行，目的是根据滑动窗口或者滚动窗口的策略将数据流划分为<code>rangeSplits</code>，<code>createSplits</code>在流式作业的生命周期中按照划分窗口的会一直执行。</li><li><code>runWithSplit</code>: 一般在执行节点节点执行，中心节点会向可执行节点发送<code>rangeSplit</code>，然后在可执行节点本地进行执行；执行完成后会将处理完的<code>splits</code>数据向下游发送。</li><li><code>commit</code>：全部的split的执行完成后，一般会向目标数据源发送<code>retract message</code>，实时动态展现结果。</li></ul><h3 id="bitsail-model" tabindex="-1"><a class="header-anchor" href="#bitsail-model" aria-hidden="true">#</a> BitSail Model</h3><p><img src="'+c+'" alt="" loading="lazy"></p><ul><li><code>createSplits</code>：BitSail通过<code>SplitCoordinator</code>模块划分<code>rangeSplits</code>，在流式作业中的生命周期中<code>createSplits</code>会周期性执行，而在批式作业中仅仅会执行一次。</li><li><code>runWithSplit</code>: 在执行节点节点执行，BitSail中执行节点包括<code>Reader</code>和<code>Writer</code>模块，中心节点会向可执行节点发送<code>rangeSplit</code>，然后在可执行节点本地进行执行；执行完成后会将处理完的<code>splits</code>数据向下游发送。</li><li><code>commit</code>：<code>writer</code>在完成数据写入后，<code>committer</code>来完成提交。在不开启<code>checkpoint</code>时，<code>commit</code>会在所有<code>writer</code>都结束后执行一次；在开启<code>checkpoint</code>时，<code>commit</code>会在每次<code>checkpoint</code>的时候都会执行一次。</li></ul><h2 id="source-connector" tabindex="-1"><a class="header-anchor" href="#source-connector" aria-hidden="true">#</a> Source Connector</h2><p><img src="'+a+`" alt="" loading="lazy"></p><ul><li>Source: 数据读取组件的生命周期管理，主要负责和框架的交互，构架作业，不参与作业真正的执行</li><li>SourceSplit: 数据读取分片；大数据处理框架的核心目的就是将大规模的数据拆分成为多个合理的Split</li><li>State：作业状态快照，当开启checkpoint之后，会保存当前执行状态。</li><li>SplitCoordinator: 既然提到了Split，就需要有相应的组件去创建、管理Split；SplitCoordinator承担了这样的角色</li><li>SourceReader: 真正负责数据读取的组件，在接收到Split后会对其进行数据读取，然后将数据传输给下一个算子</li></ul><p>Source Connector开发流程如下</p><ol><li>首先需要创建<code>Source</code>类，需要实现<code>Source</code>和<code>ParallelismComputable</code>接口，主要负责和框架的交互，构架作业，它不参与作业真正的执行</li><li><code>BitSail</code>的<code>Source</code>采用流批一体的设计思想，通过<code>getSourceBoundedness</code>方法设置作业的处理方式，通过<code>configure</code>方法定义<code>readerConfiguration</code>的配置，通过<code>createTypeInfoConverter</code>方法来进行数据类型转换，可以通过<code>FileMappingTypeInfoConverter</code>得到用户在yaml文件中自定义的数据源类型和BitSail类型的转换，实现自定义化的类型转换。</li><li>最后，定义数据源的数据分片格式<code>SourceSplit</code>类和闯将管理<code>Split</code>的角色<code>SourceSplitCoordinator</code>类</li><li>最后完成<code>SourceReader</code>实现从<code>Split</code>中进行数据的读取。</li></ol><table><thead><tr><th>Job Type</th><th>Boundedness</th></tr></thead><tbody><tr><td>batch</td><td>Boundedness.<em>BOUNDEDNESS</em></td></tr><tr><td>stream</td><td>Boundedness.<em>UNBOUNDEDNESS</em></td></tr></tbody></table><ul><li>每个<code>SourceReader</code>都在独立的线程中执行，并保证<code>SourceSplitCoordinator</code>分配给不同<code>SourceReader</code>的切片没有交集</li><li>在<code>SourceReader</code>的执行周期中，开发者只需要关注如何从构造好的切片中去读取数据，之后完成数据类型对转换，将外部数据类型转换成<code>BitSail</code>的<code>Row</code>类型传递给下游即可</li></ul><h3 id="reader示例" tabindex="-1"><a class="header-anchor" href="#reader示例" aria-hidden="true">#</a> Reader示例</h3><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public class FakeSourceReader extends SimpleSourceReaderBase&lt;Row&gt; {

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
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h2 id="sink-connector" tabindex="-1"><a class="header-anchor" href="#sink-connector" aria-hidden="true">#</a> Sink Connector</h2><p><img src="`+u+`" alt="" loading="lazy"></p><ul><li>Sink：数据写入组件的生命周期管理，主要负责和框架的交互，构架作业，它不参与作业真正的执行。</li><li>Writer：负责将接收到的数据写到外部存储。</li><li>WriterCommitter(可选)：对数据进行提交操作，来完成两阶段提交的操作；实现exactly-once的语义。</li></ul><p>开发者首先需要创建<code>Sink</code>类，实现<code>Sink</code>接口，主要负责数据写入组件的生命周期管理，构架作业。通过<code>configure</code>方法定义<code>writerConfiguration</code>的配置，通过<code>createTypeInfoConverter</code>方法来进行数据类型转换，将内部类型进行转换写到外部系统，同<code>Source</code>部分。之后我们再定义<code>Writer</code>类实现具体的数据写入逻辑，在<code>write</code>方法调用时将<code>BitSail Row</code>类型把数据写到缓存队列中，在<code>flush</code>方法调用时将缓存队列中的数据刷写到目标数据源中。</p><h3 id="writer示例" tabindex="-1"><a class="header-anchor" href="#writer示例" aria-hidden="true">#</a> Writer示例</h3><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public class PrintWriter implements Writer&lt;Row, String, Integer&gt; {
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
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h2 id="将连接器注册到配置文件中" tabindex="-1"><a class="header-anchor" href="#将连接器注册到配置文件中" aria-hidden="true">#</a> 将连接器注册到配置文件中</h2><p>为你的连接器注册配置文件，来使得框架可以在运行时动态发现它，配置文件的定义如下：</p><p>以hive为例，开发者需要在resource目录下新增一个json文件，名字示例为bitsail-connector-hive.json，只要不和其他连接器重复即可</p><div class="language-Plain line-numbers-mode" data-ext="Plain"><pre class="language-Plain"><code>{
  &quot;name&quot;: &quot;bitsail-connector-hive&quot;,
  &quot;classes&quot;: [
    &quot;com.bytedance.bitsail.connector.hive.source.HiveSource&quot;,
    &quot;com.bytedance.bitsail.connector.hive.sink.HiveSink&quot;
  ],
  &quot;libs&quot;: [
    &quot;bitsail-connector-hive-\${version}.jar&quot;
  ]
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h2 id="测试模块" tabindex="-1"><a class="header-anchor" href="#测试模块" aria-hidden="true">#</a> 测试模块</h2><p>在Source或者Sink连接器所在的模块中，新增ITCase测试用例，然后按照如下流程支持</p><ul><li>通过test container来启动相应的组件</li></ul><p><img src="`+s+`" alt="" loading="lazy"></p><ul><li>编写相应的配置文件</li></ul><div class="language-Json line-numbers-mode" data-ext="Json"><pre class="language-Json"><code>{
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
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><ul><li>通过代码EmbeddedFlinkCluster.submit来进行作业提交</li></ul><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>@Test
public void testClickhouseToPrint() throws Exception {
  BitSailConfiguration jobConf = JobConfUtils.fromClasspath(&quot;clickhouse_to_print.json&quot;);
  EmbeddedFlinkCluster.submitJob(jobConf);
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h2 id="提交pr" tabindex="-1"><a class="header-anchor" href="#提交pr" aria-hidden="true">#</a> 提交PR</h2><p>当开发者实现自己的Connector后，就可以关联自己的issue，提交PR到github上了，提交之前，开发者记得Connector添加文档，通过review之后，大家贡献的Connector就成为BitSail的一部分了，我们按照贡献程度会选取活跃的Contributor成为我们的Committer，参与BitSail社区的重大决策，希望大家积极参与！</p>`,45);function x(R,L){const o=d("RouterLink"),t=d("ExternalLinkIcon");return m(),b("div",null,[g,e("p",null,[n(o,{to:"/en/community/connector_quick_start.html"},{default:p(()=>[i("English")]),_:1}),i(" | 简体中文")]),S,_,C,k,e("p",null,[i("首先开发者需要fork BitSail仓库，详情参考"),e("a",y,[i("Fork BitSail Repo"),n(t)]),i("，之后通过git clone仓库到本地，并导入到IDE中。同时创建自己的工作分支，使用该分支开发自己的Connector。项目地址："),e("a",B,[i("https://github.com/bytedance/bitsail.git。"),n(t)])]),w])}const E=v(f,[["render",x],["__file","connector_quick_start.html.vue"]]);export{E as default};
