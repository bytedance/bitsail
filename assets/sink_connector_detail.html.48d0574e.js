import{_ as r}from"./sink_connector.3fde728e.js";import{_ as s,a as t}from"./writer_diagram.6d20d83e.js";import{_ as d}from"./_plugin-vue_export-helper.cdc0426e.js";import{o as a,c as l,a as e,d as o,w as c,b as i,e as v,r as u}from"./app.a6ab98b3.js";const m={},b=e("h1",{id:"sink-connector-详解",tabindex:"-1"},[e("a",{class:"header-anchor",href:"#sink-connector-详解","aria-hidden":"true"},"#"),i(" Sink Connector 详解")],-1),p=v('<hr><h2 id="bitsail-sink-connector交互流程介绍" tabindex="-1"><a class="header-anchor" href="#bitsail-sink-connector交互流程介绍" aria-hidden="true">#</a> BitSail Sink Connector交互流程介绍</h2><p><img src="'+r+'" alt="" loading="lazy"></p><ul><li>Sink：数据写入组件的生命周期管理，主要负责和框架的交互，构架作业，它不参与作业真正的执行。</li><li>Writer：负责将接收到的数据写到外部存储。</li><li>WriterCommitter(可选)：对数据进行提交操作，来完成两阶段提交的操作；实现exactly-once的语义。</li></ul><p>开发者首先需要创建<code>Sink</code>类，实现<code>Sink</code>接口，主要负责数据写入组件的生命周期管理，构架作业。通过<code>configure</code>方法定义<code>writerConfiguration</code>的配置，通过<code>createTypeInfoConverter</code>方法来进行数据类型转换，将内部类型进行转换写到外部系统，同<code>Source</code>部分。之后我们再定义<code>Writer</code>类实现具体的数据写入逻辑，在<code>write</code>方法调用时将<code>BitSail Row</code>类型把数据写到缓存队列中，在<code>flush</code>方法调用时将缓存队列中的数据刷写到目标数据源中。</p><h2 id="sink" tabindex="-1"><a class="header-anchor" href="#sink" aria-hidden="true">#</a> Sink</h2><p>数据写入组件的生命周期管理，主要负责和框架的交互，构架作业，它不参与作业真正的执行。</p><p>对于每一个Sink任务，我们要实现一个继承Sink接口的类。</p><p><img src="'+s+`" alt="" loading="lazy"></p><h3 id="sink接口" tabindex="-1"><a class="header-anchor" href="#sink接口" aria-hidden="true">#</a> Sink接口</h3><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public interface Sink&lt;InputT, CommitT extends Serializable, WriterStateT extends Serializable&gt; extends Serializable {

  /**
   * @return The name of writer operation.
   */
  String getWriterName();

  /**
   * Configure writer with user defined options.
   *
   * @param commonConfiguration Common options.
   * @param writerConfiguration Options for writer.
   */
  void configure(BitSailConfiguration commonConfiguration, BitSailConfiguration writerConfiguration) throws Exception;

  /**
   * Create a writer for processing elements.
   *
   * @return An initialized writer.
   */
  Writer&lt;InputT, CommitT, WriterStateT&gt; createWriter(Writer.Context&lt;WriterStateT&gt; context) throws IOException;

  /**
   * @return A converter which supports conversion from BitSail {@link TypeInfo}
   * and external engine type.
   */
  default TypeInfoConverter createTypeInfoConverter() {
    return new BitSailTypeInfoConverter();
  }

  /**
   * @return A committer for commit committable objects.
   */
  default Optional&lt;WriterCommitter&lt;CommitT&gt;&gt; createCommitter() {
    return Optional.empty();
  }

  /**
   * @return A serializer which convert committable object to byte array.
   */
  default BinarySerializer&lt;CommitT&gt; getCommittableSerializer() {
    return new SimpleBinarySerializer&lt;CommitT&gt;();
  }

  /**
   * @return A serializer which convert state object to byte array.
   */
  default BinarySerializer&lt;WriterStateT&gt; getWriteStateSerializer() {
    return new SimpleBinarySerializer&lt;WriterStateT&gt;();
  }
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="configure方法" tabindex="-1"><a class="header-anchor" href="#configure方法" aria-hidden="true">#</a> configure方法</h3><p>负责configuration的初始化，通过commonConfiguration中的配置区分流式任务或者批式任务，向Writer类传递writerConfiguration。</p><h4 id="示例" tabindex="-1"><a class="header-anchor" href="#示例" aria-hidden="true">#</a> 示例</h4><p>ElasticsearchSink：</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public void configure(BitSailConfiguration commonConfiguration, BitSailConfiguration writerConfiguration) {
  writerConf = writerConfiguration;
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="createwriter方法" tabindex="-1"><a class="header-anchor" href="#createwriter方法" aria-hidden="true">#</a> createWriter方法</h3><p>负责生成一个继承自Writer接口的connector Writer类。</p><h3 id="createtypeinfoconverter方法" tabindex="-1"><a class="header-anchor" href="#createtypeinfoconverter方法" aria-hidden="true">#</a> createTypeInfoConverter方法</h3><p>类型转换，将内部类型进行转换写到外部系统，同Source部分。</p><h3 id="createcommitter方法" tabindex="-1"><a class="header-anchor" href="#createcommitter方法" aria-hidden="true">#</a> createCommitter方法</h3><p>可选方法，书写具体数据提交逻辑，一般用于想要保证数据exactly-once语义的场景，writer在完成数据写入后，committer来完成提交，进而实现二阶段提交，详细可以参考Doris Connector的实现。</p><h2 id="writer" tabindex="-1"><a class="header-anchor" href="#writer" aria-hidden="true">#</a> Writer</h2><p>具体的数据写入逻辑</p><p><img src="`+t+`" alt="" loading="lazy"></p><h3 id="writer接口" tabindex="-1"><a class="header-anchor" href="#writer接口" aria-hidden="true">#</a> Writer接口</h3><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public interface Writer&lt;InputT, CommT, WriterStateT&gt; extends Serializable, Closeable {

  /**
   * Output an element to target source.
   *
   * @param element Input data from upstream.
   */
  void write(InputT element) throws IOException;

  /**
   * Flush buffered input data to target source.
   *
   * @param endOfInput Flag indicates if all input data are delivered.
   */
  void flush(boolean endOfInput) throws IOException;

  /**
   * Prepare commit information before snapshotting when checkpoint is triggerred.
   *
   * @return Information to commit in this checkpoint.
   * @throws IOException Exceptions encountered when preparing committable information.
   */
  List&lt;CommT&gt; prepareCommit() throws IOException;

  /**
   * Do snapshot for at each checkpoint.
   *
   * @param checkpointId The id of checkpoint when snapshot triggered.
   * @return The current state of writer.
   * @throws IOException Exceptions encountered when snapshotting.
   */
  default List&lt;WriterStateT&gt; snapshotState(long checkpointId) throws IOException {
    return Collections.emptyList();
  }

  /**
   * Closing writer when operator is closed.
   *
   * @throws IOException Exception encountered when closing writer.
   */
  default void close() throws IOException {

  }

  interface Context&lt;WriterStateT&gt; extends Serializable {

    TypeInfo&lt;?&gt;[] getTypeInfos();

    int getIndexOfSubTaskId();

    boolean isRestored();

    List&lt;WriterStateT&gt; getRestoreStates();
  }
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="构造方法" tabindex="-1"><a class="header-anchor" href="#构造方法" aria-hidden="true">#</a> 构造方法</h3><p>根据writerConfiguration配置初始化数据源的连接对象。</p><h4 id="示例-1" tabindex="-1"><a class="header-anchor" href="#示例-1" aria-hidden="true">#</a> 示例</h4><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public RedisWriter(BitSailConfiguration writerConfiguration) {
  // initialize ttl
  int ttl = writerConfiguration.getUnNecessaryOption(RedisWriterOptions.TTL, -1);
  TtlType ttlType;
  try {
    ttlType = TtlType.valueOf(StringUtils.upperCase(writerConfiguration.get(RedisWriterOptions.TTL_TYPE)));
  } catch (IllegalArgumentException e) {
    throw BitSailException.asBitSailException(RedisPluginErrorCode.ILLEGAL_VALUE,
        String.format(&quot;unknown ttl type: %s&quot;, writerConfiguration.get(RedisWriterOptions.TTL_TYPE)));
  }
  int ttlInSeconds = ttl &lt; 0 ? -1 : ttl * ttlType.getContainSeconds();
  log.info(&quot;ttl is {}(s)&quot;, ttlInSeconds);

  // initialize commandDescription
  String redisDataType = StringUtils.upperCase(writerConfiguration.get(RedisWriterOptions.REDIS_DATA_TYPE));
  String additionalKey = writerConfiguration.getUnNecessaryOption(RedisWriterOptions.ADDITIONAL_KEY, &quot;default_redis_key&quot;);
  this.commandDescription = initJedisCommandDescription(redisDataType, ttlInSeconds, additionalKey);
  this.columnSize = writerConfiguration.get(RedisWriterOptions.COLUMNS).size();

  // initialize jedis pool
  JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
  jedisPoolConfig.setMaxTotal(writerConfiguration.get(RedisWriterOptions.JEDIS_POOL_MAX_TOTAL_CONNECTIONS));
  jedisPoolConfig.setMaxIdle(writerConfiguration.get(RedisWriterOptions.JEDIS_POOL_MAX_IDLE_CONNECTIONS));
  jedisPoolConfig.setMinIdle(writerConfiguration.get(RedisWriterOptions.JEDIS_POOL_MIN_IDLE_CONNECTIONS));
  jedisPoolConfig.setMaxWait(Duration.ofMillis(writerConfiguration.get(RedisWriterOptions.JEDIS_POOL_MAX_WAIT_TIME_IN_MILLIS)));

  String redisHost = writerConfiguration.getNecessaryOption(RedisWriterOptions.HOST, RedisPluginErrorCode.REQUIRED_VALUE);
  int redisPort = writerConfiguration.getNecessaryOption(RedisWriterOptions.PORT, RedisPluginErrorCode.REQUIRED_VALUE);
  String redisPassword = writerConfiguration.get(RedisWriterOptions.PASSWORD);
  int timeout = writerConfiguration.get(RedisWriterOptions.CLIENT_TIMEOUT_MS);

  if (StringUtils.isEmpty(redisPassword)) {
    this.jedisPool = new JedisPool(jedisPoolConfig, redisHost, redisPort, timeout);
  } else {
    this.jedisPool = new JedisPool(jedisPoolConfig, redisHost, redisPort, timeout, redisPassword);
  }

  // initialize record queue
  int batchSize = writerConfiguration.get(RedisWriterOptions.WRITE_BATCH_INTERVAL);
  this.recordQueue = new CircularFifoQueue&lt;&gt;(batchSize);

  this.logSampleInterval = writerConfiguration.get(RedisWriterOptions.LOG_SAMPLE_INTERVAL);
  this.jedisFetcher = RetryerBuilder.&lt;Jedis&gt;newBuilder()
      .retryIfResult(Objects::isNull)
      .retryIfRuntimeException()
      .withStopStrategy(StopStrategies.stopAfterAttempt(3))
      .withWaitStrategy(WaitStrategies.exponentialWait(100, 5, TimeUnit.MINUTES))
      .build()
      .wrap(jedisPool::getResource);

  this.maxAttemptCount = writerConfiguration.get(RedisWriterOptions.MAX_ATTEMPT_COUNT);
  this.retryer = RetryerBuilder.&lt;Boolean&gt;newBuilder()
      .retryIfResult(needRetry -&gt; Objects.equals(needRetry, true))
      .retryIfException(e -&gt; !(e instanceof BitSailException))
      .withWaitStrategy(WaitStrategies.fixedWait(3, TimeUnit.SECONDS))
      .withStopStrategy(StopStrategies.stopAfterAttempt(maxAttemptCount))
      .build();
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="write方法" tabindex="-1"><a class="header-anchor" href="#write方法" aria-hidden="true">#</a> write方法</h3><p>该方法调用时会将BitSail Row类型把数据写到缓存队列中，也可以在这里对Row类型数据进行各种格式预处理。直接存储到缓存队列中，或者进行加工处理。如果这里设定了缓存队列的大小，那么在缓存队列写满后要调用flush进行刷写。</p><h4 id="示例-2" tabindex="-1"><a class="header-anchor" href="#示例-2" aria-hidden="true">#</a> 示例</h4><p>redis：将BitSail Row格式的数据直接存储到一定大小的缓存队列中</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public void write(Row record) throws IOException {
  validate(record);
  this.recordQueue.add(record);
  if (recordQueue.isAtFullCapacity()) {
    flush(false);
  }
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p>Druid：将BitSail Row格式的数据做格式预处理，转化到StringBuffer中储存起来。</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>@Override
public void write(final Row element) {
  final StringJoiner joiner = new StringJoiner(DEFAULT_FIELD_DELIMITER, &quot;&quot;, &quot;&quot;);
  for (int i = 0; i &lt; element.getArity(); i++) {
    final Object v = element.getField(i);
    if (v != null) {
      joiner.add(v.toString());
    }
  }
  // timestamp column is a required field to add in Druid.
  // See https://druid.apache.org/docs/24.0.0/ingestion/data-model.html#primary-timestamp
  joiner.add(String.valueOf(processTime));
  data.append(joiner);
  data.append(DEFAULT_LINE_DELIMITER);
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="flush方法" tabindex="-1"><a class="header-anchor" href="#flush方法" aria-hidden="true">#</a> flush方法</h3><p>该方法中主要实现将write方法的缓存中的数据刷写到目标数据源中。</p><h4 id="示例-3" tabindex="-1"><a class="header-anchor" href="#示例-3" aria-hidden="true">#</a> 示例</h4><p>redis：将缓存队列中的BitSail Row格式的数据刷写到目标数据源中。</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public void flush(boolean endOfInput) throws IOException {
  processorId++;
  try (PipelineProcessor processor = genPipelineProcessor(recordQueue.size(), this.complexTypeWithTtl)) {
    Row record;
    while ((record = recordQueue.poll()) != null) {

      String key = (String) record.getField(0);
      String value = (String) record.getField(1);
      String scoreOrHashKey = value;
      if (columnSize == SORTED_SET_OR_HASH_COLUMN_SIZE) {
        value = (String) record.getField(2);
        // Replace empty key with additionalKey in sorted set and hash.
        if (key.length() == 0) {
          key = commandDescription.getAdditionalKey();
        }
      }

      if (commandDescription.getJedisCommand() == JedisCommand.ZADD) {
        // sorted set
        processor.addInitialCommand(new Command(commandDescription, key.getBytes(), parseScoreFromString(scoreOrHashKey), value.getBytes()));
      } else if (commandDescription.getJedisCommand() == JedisCommand.HSET) {
        // hash
        processor.addInitialCommand(new Command(commandDescription, key.getBytes(), scoreOrHashKey.getBytes(), value.getBytes()));
      } else if (commandDescription.getJedisCommand() == JedisCommand.HMSET) {
        //mhset
        if ((record.getArity() - 1) % 2 != 0) {
          throw new BitSailException(CONVERT_NOT_SUPPORT, &quot;Inconsistent data entry.&quot;);
        }
        List&lt;byte[]&gt; datas = Arrays.stream(record.getFields())
            .collect(Collectors.toList()).stream().map(o -&gt; ((String) o).getBytes())
            .collect(Collectors.toList()).subList(1, record.getFields().length);
        Map&lt;byte[], byte[]&gt; map = new HashMap&lt;&gt;((record.getArity() - 1) / 2);
        for (int index = 0; index &lt; datas.size(); index = index + 2) {
          map.put(datas.get(index), datas.get(index + 1));
        }
        processor.addInitialCommand(new Command(commandDescription, key.getBytes(), map));
      } else {
        // set and string
        processor.addInitialCommand(new Command(commandDescription, key.getBytes(), value.getBytes()));
      }
    }
    retryer.call(processor::run);
  } catch (ExecutionException | RetryException e) {
    if (e.getCause() instanceof BitSailException) {
      throw (BitSailException) e.getCause();
    } else if (e.getCause() instanceof RedisUnexpectedException) {
      throw (RedisUnexpectedException) e.getCause();
    }
    throw e;
  } catch (IOException e) {
    throw new RuntimeException(&quot;Error while init jedis client.&quot;, e);
  }
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p>Druid：使用HTTP post方式提交sink作业给数据源。</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>private HttpURLConnection provideHttpURLConnection(final String coordinatorURL) throws IOException {
    final URL url = new URL(&quot;http://&quot; + coordinatorURL + DRUID_ENDPOINT);
    final HttpURLConnection con = (HttpURLConnection) url.openConnection();
    con.setRequestMethod(&quot;POST&quot;);
    con.setRequestProperty(&quot;Content-Type&quot;, &quot;application/json&quot;);
    con.setRequestProperty(&quot;Accept&quot;, &quot;application/json, text/plain, */*&quot;);
    con.setDoOutput(true);
    return con;
  }
  
  public void flush(final boolean endOfInput) throws IOException {
    final ParallelIndexIOConfig ioConfig = provideDruidIOConfig(data);
    final ParallelIndexSupervisorTask indexTask = provideIndexTask(ioConfig);
    final String inputJSON = provideInputJSONString(indexTask);
    final byte[] input = inputJSON.getBytes();
    try (final OutputStream os = httpURLConnection.getOutputStream()) {
      os.write(input, 0, input.length);
    }
    try (final BufferedReader br =
                 new BufferedReader(new InputStreamReader(httpURLConnection.getInputStream(), StandardCharsets.UTF_8))) {
      final StringBuilder response = new StringBuilder();
      String responseLine;
      while ((responseLine = br.readLine()) != null) {
        response.append(responseLine.trim());
      }
      LOG.info(&quot;Druid write task has been sent, and the response is {}&quot;, response);
    }
  }
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="close方法" tabindex="-1"><a class="header-anchor" href="#close方法" aria-hidden="true">#</a> close方法</h3><p>关闭之前创建的各种目标数据源连接对象。</p><h4 id="示例-4" tabindex="-1"><a class="header-anchor" href="#示例-4" aria-hidden="true">#</a> 示例</h4><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public void close() throws IOException {
  bulkProcessor.close();
  restClient.close();
  checkErrorAndRethrow();
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div>`,49);function g(h,f){const n=u("RouterLink");return a(),l("div",null,[b,e("p",null,[o(n,{to:"/en/community/sink_connector_detail.html"},{default:c(()=>[i("English")]),_:1}),i(" | 简体中文")]),p])}const y=d(m,[["render",g],["__file","sink_connector_detail.html.vue"]]);export{y as default};
