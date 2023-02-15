import{_ as t}from"./sink_connector.3fde728e.js";import{_ as r,a}from"./writer_diagram.6d20d83e.js";import{_ as s}from"./_plugin-vue_export-helper.cdc0426e.js";import{o as d,c as l,a as e,b as i,d as o,w as c,e as v,r as u}from"./app.adeb8394.js";const m={},b=e("h1",{id:"sink-connector-details",tabindex:"-1"},[e("a",{class:"header-anchor",href:"#sink-connector-details","aria-hidden":"true"},"#"),i(" Sink Connector Details")],-1),h=v('<hr><h2 id="introduction" tabindex="-1"><a class="header-anchor" href="#introduction" aria-hidden="true">#</a> Introduction</h2><p><img src="'+t+'" alt="" loading="lazy"></p><ul><li>Sink: life cycle management of data writing components, mainly responsible for interaction with the framework, framing jobs, it does not participate in the actual execution of jobs.</li><li>Writer: responsible for writing the received data to external storage.</li><li>WriterCommitter (optional): Commit the data to complete the two-phase commit operation; realize the semantics of exactly-once.</li></ul><p>Developers first need to create a <code>Sink</code> class and implement the <code>Sink interface</code>, which is mainly responsible for the life cycle management of the data writing component and the construction of the job. Define the configuration of <code>writerConfiguration</code> through the configure method, perform data type conversion through the <code>createTypeInfoConverter</code> method, and <code>write</code> the internal type conversion to the external system, the same as the <code>Source</code> part. Then we define the <code>Writer</code> class to implement the specific data writing logic. When the <code>write</code> method is called, the <code>BitSail Row</code> type writes the data into the cache queue, and when the <code>flush</code> method is called, the data in the cache queue is flushed to the target data source.</p><h2 id="sink" tabindex="-1"><a class="header-anchor" href="#sink" aria-hidden="true">#</a> Sink</h2><p>The life cycle management of the data writing component is mainly responsible for the interaction with the framework and the construction of the job. It does not participate in the actual execution of the job.</p><p>For each Sink task, we need to implement a class that inherits the Sink interface.</p><p><img src="'+r+`" alt="" loading="lazy"></p><h3 id="sink-interface" tabindex="-1"><a class="header-anchor" href="#sink-interface" aria-hidden="true">#</a> Sink Interface</h3><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public interface Sink&lt;InputT, CommitT extends Serializable, WriterStateT extends Serializable&gt; extends Serializable {

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
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="configure-method" tabindex="-1"><a class="header-anchor" href="#configure-method" aria-hidden="true">#</a> configure method</h3><p>Responsible for configuration initialization, usually extracting necessary configuration from commonConfiguration and writerConfiguration.</p><h4 id="example" tabindex="-1"><a class="header-anchor" href="#example" aria-hidden="true">#</a> example</h4><p>ElasticsearchSink:</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public void configure(BitSailConfiguration commonConfiguration, BitSailConfiguration writerConfiguration) {
  writerConf = writerConfiguration;
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="createwriter-method" tabindex="-1"><a class="header-anchor" href="#createwriter-method" aria-hidden="true">#</a> createWriter method</h3><p>Responsible for generating a connector Writer class inherited from the Writer interface. Pass in construction configuration parameters as needed, and note that the passed in parameters must be serializable.</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>@Override
public Writer&lt;Row, CommitT, EmptyState&gt; createWriter(Writer.Context&lt;EmptyState&gt; context) {
  return new RedisWriter&lt;&gt;(redisOptions, jedisPoolOptions);
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="createtypeinfoconverter-method" tabindex="-1"><a class="header-anchor" href="#createtypeinfoconverter-method" aria-hidden="true">#</a> createTypeInfoConverter method</h3><p>Type conversion, convert the internal type and write it to the external system, same as the Source part.</p><h3 id="createcommitter-method" tabindex="-1"><a class="header-anchor" href="#createcommitter-method" aria-hidden="true">#</a> createCommitter method</h3><p>The optional method is to write the specific data submission logic, which is generally used in scenarios where the data exactly-once semantics needs to be guaranteed. After the writer completes the data writing, the committer completes the submission, and then realizes the two-phase submission. For details, please refer to the implementation of Doris Connector.</p><h2 id="writer" tabindex="-1"><a class="header-anchor" href="#writer" aria-hidden="true">#</a> Writer</h2><p>specific data write logic</p><p><img src="`+a+`" alt="" loading="lazy"></p><h3 id="writer-interface" tabindex="-1"><a class="header-anchor" href="#writer-interface" aria-hidden="true">#</a> Writer Interface</h3><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public interface Writer&lt;InputT, CommT, WriterStateT&gt; extends Serializable, Closeable {

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
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="construction-method" tabindex="-1"><a class="header-anchor" href="#construction-method" aria-hidden="true">#</a> Construction method</h3><p>Initialize the connection object of the data source according to the configuration, and establish a connection with the target data source.</p><h4 id="example-1" tabindex="-1"><a class="header-anchor" href="#example-1" aria-hidden="true">#</a> example</h4><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public RedisWriter(BitSailConfiguration writerConfiguration) {
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
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="write-method" tabindex="-1"><a class="header-anchor" href="#write-method" aria-hidden="true">#</a> write method</h3><p>When this method is called, the BitSail Row type data will be written to the cache queue, and various formats of Row type data can also be preprocessed here. If the size of the cache queue is set here, then flush is called after the cache queue is full.</p><h4 id="example-2" tabindex="-1"><a class="header-anchor" href="#example-2" aria-hidden="true">#</a> example</h4><p>redis：Store data in <code>BitSail Row</code> format directly in a cache queue of a certain size</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public void write(Row record) throws IOException {
  validate(record);
  this.recordQueue.add(record);
  if (recordQueue.isAtFullCapacity()) {
    flush(false);
  }
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p>Druid：Preprocess the data in <code>BitSail Row</code> format and convert it into <code>StringBuffer</code> for storage.</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>@Override
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
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="flush-method" tabindex="-1"><a class="header-anchor" href="#flush-method" aria-hidden="true">#</a> flush method</h3><p>This method mainly implements flushing the data in the cache of the <code>write</code> method to the target data source.</p><h4 id="example-3" tabindex="-1"><a class="header-anchor" href="#example-3" aria-hidden="true">#</a> example</h4><p>redis: flush the BitSail Row format data in the cache queue to the target data source.</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public void flush(boolean endOfInput) throws IOException {
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
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p>Druid: Submit the sink job to the data source using HTTP post.</p><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>private HttpURLConnection provideHttpURLConnection(final String coordinatorURL) throws IOException {
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
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="close-method" tabindex="-1"><a class="header-anchor" href="#close-method" aria-hidden="true">#</a> close method</h3><p>Closes any previously created target data source connection objects.</p><h4 id="example-4" tabindex="-1"><a class="header-anchor" href="#example-4" aria-hidden="true">#</a> example</h4><div class="language-Java line-numbers-mode" data-ext="Java"><pre class="language-Java"><code>public void close() throws IOException {
  bulkProcessor.close();
  restClient.close();
  checkErrorAndRethrow();
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div>`,50);function p(g,f){const n=u("RouterLink");return d(),l("div",null,[b,e("p",null,[i("English | "),o(n,{to:"/zh/community/sink_connector_detail.html"},{default:c(()=>[i("简体中文")]),_:1})]),h])}const y=s(m,[["render",p],["__file","sink_connector_detail.html.vue"]]);export{y as default};
