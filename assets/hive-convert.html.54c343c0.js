import{_ as e}from"./_plugin-vue_export-helper.cdc0426e.js";import{o as n,c as a,e as s}from"./app.a8d4311a.js";const i={},t=s(`<h1 id="bitsail-convertion-flink-hive" tabindex="-1"><a class="header-anchor" href="#bitsail-convertion-flink-hive" aria-hidden="true">#</a> bitsail-convertion-flink-hive</h1><hr><p>本模块中的<code>HivewritableExtractor</code>支持将bitsail支持的<code>Row</code>转化为hive <code>Writable</code>数据。 转化完成后，用户可使用<code>org.apache.hadoop.hive.ql.exec.RecordWriter</code>将转化后的<code>Writable</code>数据方便地写入hive中。</p><p>下文将详细介绍<code>GeneralWritableExtractor</code>，一种<code>HivewritableExtractor</code>的通用实现，可用于多种存储格式的hive表，例如parquet、orc、text等。</p><h2 id="支持的数据类型" tabindex="-1"><a class="header-anchor" href="#支持的数据类型" aria-hidden="true">#</a> 支持的数据类型</h2><p><code>GeneralWritableExtractor</code>支持常见的hive基础数据类型，以及List、Map、Struct复杂数据类型。 常见的基础数据类型包括：</p><div class="language-text line-numbers-mode" data-ext="text"><pre class="language-text"><code> TINYINT
 SMALLINT
 INT
 BIGINT
 BOOLEAN
 FLOAT
 DOUBLE
 DECIMAL
 STRING
 BINARY
 DATE
 TIMESTAMP
 CHAR
 VARCHAR
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h2 id="转化功能" tabindex="-1"><a class="header-anchor" href="#转化功能" aria-hidden="true">#</a> 转化功能</h2><p>除了按照类型进行基础的转化外，<code>GeneralWritableExtractor</code>还支持通过选项支持其他的一些转化功能。 这些选项统一在 <code>com.bytedance.bitsail.conversion.hive.ConvertToHiveObjectOptions</code> 管理，主要包括以下几种:</p><div class="language-java line-numbers-mode" data-ext="java"><pre class="language-java"><code><span class="token keyword">public</span> <span class="token keyword">class</span> <span class="token class-name">ConvertToHiveObjectOptions</span> <span class="token keyword">implements</span> <span class="token class-name">Serializable</span> <span class="token punctuation">{</span>

  <span class="token keyword">private</span> <span class="token keyword">boolean</span> convertErrorColumnAsNull<span class="token punctuation">;</span>
  <span class="token keyword">private</span> <span class="token keyword">boolean</span> dateTypeToStringAsLong<span class="token punctuation">;</span>
  <span class="token keyword">private</span> <span class="token keyword">boolean</span> nullStringAsNull<span class="token punctuation">;</span>
  <span class="token keyword">private</span> <span class="token class-name">DatePrecision</span> datePrecision<span class="token punctuation">;</span>

  <span class="token keyword">public</span> <span class="token keyword">enum</span> <span class="token class-name">DatePrecision</span> <span class="token punctuation">{</span>
    <span class="token constant">SECOND</span><span class="token punctuation">,</span> <span class="token constant">MILLISECOND</span>
  <span class="token punctuation">}</span>
<span class="token punctuation">}</span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="converterrorcolumnasnull" tabindex="-1"><a class="header-anchor" href="#converterrorcolumnasnull" aria-hidden="true">#</a> convertErrorColumnAsNull</h3><p>当数据转化出现错误时，此选项可决定报出错误或者忽略错误并转化为null。</p><p>例如，在转化字符串 <code>&quot;123k.i123&quot;</code> 为Double类型的hive数据时，由于字符串不能被识别为一个浮点数，会产生报错。</p><ul><li>若<code>convertErrorColumnAsNull=true</code>，则忽略此报错，并将此字符串转化为<code>null</code>。</li><li>若<code>convertErrorColumnAsNull=false</code>，则报出转化错误。</li></ul><h3 id="datetypetostringaslong" tabindex="-1"><a class="header-anchor" href="#datetypetostringaslong" aria-hidden="true">#</a> dateTypeToStringAsLong</h3><p>若传入的<code>Row</code>中需要转化的字段类型为<code>com.bytedance.bitsail.common.column.DateColumn</code>，且该column使用<code>java.sql.Timestamp</code> 初始化时，则该字段会被转化为时间戳数据。</p><h3 id="nullstringasnull" tabindex="-1"><a class="header-anchor" href="#nullstringasnull" aria-hidden="true">#</a> nullStringAsNull</h3><p>若传入的<code>Row</code>中需要转化的字段数据为null，且目标hive数据类型为字符串时，用户可选择转化成 null 或者 空字符串 &quot;&quot;。</p><table><thead><tr><th>参数</th><th>值</th><th>转化</th></tr></thead><tbody><tr><td><code>nullStringAsNull</code></td><td><code>true</code></td><td><code>null</code> -&gt; <code>null</code></td></tr><tr><td><code>nullStringAsNull</code></td><td><code>false</code></td><td><code>null</code> -&gt; <code>&quot;&quot;</code></td></tr></tbody></table><h3 id="dateprecision" tabindex="-1"><a class="header-anchor" href="#dateprecision" aria-hidden="true">#</a> datePrecision</h3><p>此选项决定<code>Date</code>数据类型转化为时间戳时的精度，可选值为<code>SECOND</code>和<code>MILLISECOND</code>。</p><p>例如，在转化日期&quot;2022-01-01 12:34:56&quot;为时间戳时，根据<code>datePrecision</code>，会返回不同的值：</p><ul><li><code>datePrecision=SECOND</code>: 1641011696</li><li><code>datePrecision=MILLISECOND</code>: 1641011696000</li></ul><h2 id="如何使用" tabindex="-1"><a class="header-anchor" href="#如何使用" aria-hidden="true">#</a> 如何使用</h2><p>下面介绍如何使用<code>GeneralHiveExtractor</code>。</p><h3 id="初始化" tabindex="-1"><a class="header-anchor" href="#初始化" aria-hidden="true">#</a> 初始化</h3><p>在创建<code>GeneralHiveExtractor</code>实例后，还需要以下几步进行初始化:</p><h4 id="_1-设置字段映射和字段名" tabindex="-1"><a class="header-anchor" href="#_1-设置字段映射和字段名" aria-hidden="true">#</a> 1. 设置<b>字段映射</b>和<b>字段名</b></h4><p>fieldNames和columnMapping共同决定了<code>Row</code>中的字段写入hive的顺序。</p><ul><li>columnMapping: 以Map形式存储的字段名到hive中字段位置的映射。</li><li>fieldNames: 需要转化的<code>Row</code>中的字段名。</li></ul><p>下面举例说明这两个参数如何设置的:</p><ul><li>要写入的hivec测试表<span id="hive_example">hive_example</span>结构如下:</li></ul><table><thead><tr><th>字段名</th><th>字段类型</th><th>index</th></tr></thead><tbody><tr><td><code>field_c</code></td><td><code>STRING</code></td><td>0</td></tr><tr><td><code>field_b</code></td><td><code>STRING</code></td><td>1</td></tr><tr><td><code>field_a</code></td><td><code>STRING</code></td><td>2</td></tr></tbody></table><ul><li>要转化的<code>Row</code>内容如下:</li></ul><div class="language-json line-numbers-mode" data-ext="json"><pre class="language-json"><code><span class="token punctuation">[</span>
  <span class="token punctuation">{</span><span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;row_field_0&quot;</span><span class="token punctuation">,</span> <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;string&quot;</span><span class="token punctuation">,</span> <span class="token property">&quot;data&quot;</span><span class="token operator">:</span> <span class="token string">&quot;0&quot;</span><span class="token punctuation">}</span><span class="token punctuation">,</span>
  <span class="token punctuation">{</span><span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;row_field_1&quot;</span><span class="token punctuation">,</span> <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;string&quot;</span><span class="token punctuation">,</span> <span class="token property">&quot;data&quot;</span><span class="token operator">:</span> <span class="token string">&quot;1&quot;</span><span class="token punctuation">}</span><span class="token punctuation">,</span>
  <span class="token punctuation">{</span><span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;row_field_2&quot;</span><span class="token punctuation">,</span> <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;string&quot;</span><span class="token punctuation">,</span> <span class="token property">&quot;data&quot;</span><span class="token operator">:</span> <span class="token string">&quot;2&quot;</span><span class="token punctuation">}</span>
<span class="token punctuation">]</span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><ul><li>根据<code>Row</code>内容，<code>fieldNames</code>的内容需要设置为:</li></ul><div class="language-java line-numbers-mode" data-ext="java"><pre class="language-java"><code><span class="token class-name">String</span> fieldNames <span class="token operator">=</span> <span class="token punctuation">{</span><span class="token string">&quot;field_0&quot;</span><span class="token punctuation">,</span> <span class="token string">&quot;field_1&quot;</span><span class="token punctuation">,</span> <span class="token string">&quot;field_2&quot;</span><span class="token punctuation">}</span><span class="token punctuation">;</span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div></div></div><ul><li>若想完成 <code>row_field_0-&gt;field_a</code>,<code>row_field_1-&gt;field_b</code>, <code>row_field_2-&gt;field_c</code>的映射，则将columnMapping设置为如下值:</li></ul><div class="language-text line-numbers-mode" data-ext="text"><pre class="language-text"><code>Map&lt;String, Integer&gt; columnMapping = ImmutableMap.of(
  &quot;row_field_0&quot;, 2,
  &quot;row_field_1&quot;, 1,
  &quot;row_field_2&quot;, 0
);
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h4 id="_2-设置转化参数" tabindex="-1"><a class="header-anchor" href="#_2-设置转化参数" aria-hidden="true">#</a> 2. 设置转化参数</h4><p>用户自行构建用于转化的<code>ConvertToHiveObjectOptions</code>后，可设置到<code>GeneralWritableExtractor</code>。</p><div class="language-text line-numbers-mode" data-ext="text"><pre class="language-text"><code>ConvertToHiveObjectOptions options = ConvertToHiveObjectOptions.builder()
  .convertErrorColumnAsNull(false)
  .dateTypeToStringAsLong(false)
  .datePrecision(ConvertToHiveObjectOptions.DatePrecision.SECOND)
  .nullStringAsNull(false)
  .build();

  hiveWritableExtractor.initConvertOptions(options);
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h4 id="_3-初始化objectinspector" tabindex="-1"><a class="header-anchor" href="#_3-初始化objectinspector" aria-hidden="true">#</a> 3. 初始化ObjectInspector</h4><p><code>GeneralWritableExtractor</code>提供了接口初始化用于转化的ObjectInspector。</p><div class="language-text line-numbers-mode" data-ext="text"><pre class="language-text"><code>public SettableStructObjectInspector createObjectInspector(final String columnNames, final String columnTypes);
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div></div></div><p>其中:</p><ul><li><code>columnNames</code>: hive中字段名以 <code>,</code> 分隔组成的字符串， 顺序和hive中一致。</li><li><code>columnsType</code>: hive中字段类型以 <code>,</code> 分隔组成的字符串， 顺序和hive中一致。</li></ul><p>以上面的 <a href="#hive_example">hive_example</a>为例:</p><div class="language-text line-numbers-mode" data-ext="text"><pre class="language-text"><code>String columnNames = &quot;field_c,field_b,field_a&quot;;
String columnTypes = &quot;string,string,string&quot;;
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="示例代码" tabindex="-1"><a class="header-anchor" href="#示例代码" aria-hidden="true">#</a> 示例代码</h3><div class="language-text line-numbers-mode" data-ext="text"><pre class="language-text"><code>/**
   * Hive table schema is:
   *     | field_name | field_type | field_index |
   *     | field_0    | bigint     | 0           |
   *     | field_1    | string     | 1           |
   *     | field_2    | double     | 2           |
   * Hive serde class is: org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe
   *
   * Row structure is:
   *    {
   *      (&quot;name&quot;:&quot;field_a&quot;, &quot;type&quot;:&quot;long&quot;, &quot;data&quot;:100),
   *      (&quot;name&quot;:&quot;field_b&quot;, &quot;type&quot;:&quot;string&quot;, &quot;data&quot;:&quot;str&quot;),
   *      (&quot;name&quot;:&quot;field_c&quot;, &quot;type&quot;:&quot;double&quot;, &quot;data&quot;:3.14),
   *    }
   * 
   * @param serDe Initialized serializer. See \`org.apache.hadoop.hive.serde2.Serializer\`.
   * @param hiveWriter Initialized record writer. See \`org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter\`.
   */
  public void transformAndWrite(Serializer serDe, FileSinkOperator.RecordWriter hiveWriter) throws Exception {
    // 0. Initialize parameters.
    String[] fieldNames = {&quot;field_a&quot;, &quot;field_b&quot;, &quot;field_c&quot;};
    Map&lt;String, Integer&gt; columnMapping = ImmutableMap.of(
      &quot;field_a&quot;, 0,
      &quot;field_b&quot;, 1,
      &quot;field_c&quot;, 2
    );
    ConvertToHiveObjectOptions options = ConvertToHiveObjectOptions.builder()
      .convertErrorColumnAsNull(false)
      .dateTypeToStringAsLong(false)
      .datePrecision(ConvertToHiveObjectOptions.DatePrecision.SECOND)
      .nullStringAsNull(false)
      .build();
    String hiveColumnNames = &quot;field_0,field_1,field_2&quot;;
    String hiveColumnTypes = &quot;bigint,string,double&quot;;

    // 1. Prepare a row.
    Row row = new Row(3);
    row.setField(0, new LongColumn(100));
    row.setField(1, new StringColumn(&quot;str&quot;));
    row.setField(2, new DoubleColumn(3.14));

    // 2. Create GeneralWritableExtractor instance.
    GeneralWritableExtractor extractor = new GeneralWritableExtractor();
    extractor.setColumnMapping(columnMapping);
    extractor.setFieldNames(fieldNames);
    extractor.initConvertOptions(options);
    ObjectInspector inspector = extractor.createObjectInspector(hiveColumnNames, hiveColumnTypes);

    // 3. Transform row and write it to hive.
    Object hiveRow = extractor.createRowObject(row);
    Writable writable = serDe.serialize(hiveRow, inspector);
    hiveWriter.write(writable);
  }
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div>`,51),o=[t];function l(d,r){return n(),a("div",null,o)}const p=e(i,[["render",l],["__file","hive-convert.html.vue"]]);export{p as default};
