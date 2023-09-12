import{_ as a}from"./_plugin-vue_export-helper.cdc0426e.js";import{o as t,c as s,a as e,b as n,d as o,w as l,e as r,r as d}from"./app.c1da3360.js";const c={},u=e("h1",{id:"bitsail-convertion-flink-hive",tabindex:"-1"},[e("a",{class:"header-anchor",href:"#bitsail-convertion-flink-hive","aria-hidden":"true"},"#"),n(" bitsail-convertion-flink-hive")],-1),p=e("hr",null,null,-1),v=r(`<p>The <code>HiveWritableExtractor</code> in this module supports converting <code>Row</code> into hive <code>Writable</code> data. After the converting, users can easily use <code>org.apache.hadoop.hive.ql.exec.RecordWriter</code> to write the converted data into hive.</p><p>The following sections will introduce <code>GeneralWritableExtractor</code>, an implementation of <code>HiveWritableExtractor</code>, which can be used for hive tables of various storage formats, such as parquet, orc, text, <i>etc.</i>.</p><h2 id="supported-data-types" tabindex="-1"><a class="header-anchor" href="#supported-data-types" aria-hidden="true">#</a> Supported data types</h2><p><code>GeneralWritableExtractor</code> support common hive basic data types, as well as List, Map, Struct complex data types.</p><p>Common data types include:</p><div class="language-text line-numbers-mode" data-ext="text"><pre class="language-text"><code> TINYINT
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
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h2 id="conversion" tabindex="-1"><a class="header-anchor" href="#conversion" aria-hidden="true">#</a> Conversion</h2><p>In addition to basic conversions by type，<code>GeneralWritableExtractor</code> also supports some other conversion functions according to options. These options are managed in <code>com.bytedance.bitsail.conversion.hive.ConvertToHiveObjectOptions</code>, including:</p><div class="language-java line-numbers-mode" data-ext="java"><pre class="language-java"><code><span class="token keyword">public</span> <span class="token keyword">class</span> <span class="token class-name">ConvertToHiveObjectOptions</span> <span class="token keyword">implements</span> <span class="token class-name">Serializable</span> <span class="token punctuation">{</span>

  <span class="token keyword">private</span> <span class="token keyword">boolean</span> convertErrorColumnAsNull<span class="token punctuation">;</span>
  <span class="token keyword">private</span> <span class="token keyword">boolean</span> dateTypeToStringAsLong<span class="token punctuation">;</span>
  <span class="token keyword">private</span> <span class="token keyword">boolean</span> nullStringAsNull<span class="token punctuation">;</span>
  <span class="token keyword">private</span> <span class="token class-name">DatePrecision</span> datePrecision<span class="token punctuation">;</span>

  <span class="token keyword">public</span> <span class="token keyword">enum</span> <span class="token class-name">DatePrecision</span> <span class="token punctuation">{</span>
    <span class="token constant">SECOND</span><span class="token punctuation">,</span> <span class="token constant">MILLISECOND</span>
  <span class="token punctuation">}</span>
<span class="token punctuation">}</span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="converterrorcolumnasnull" tabindex="-1"><a class="header-anchor" href="#converterrorcolumnasnull" aria-hidden="true">#</a> convertErrorColumnAsNull</h3><p>When there is an error in the data conversion, this option can decide to report the error or ignore the error and convert to null.</p><p>For example, when converting a string &quot;123k.i123&quot; to double-type hive data, an error will be reported because the string cannot be recognized as a float number.</p><ul><li>If <code>convertErrorColumnAsNull=true</code>, ignore this error and convert this string to null.</li><li>If <code>convertErrorColumnAsNull=false</code>, a converting error exception is reported.</li></ul><h3 id="datetypetostringaslong" tabindex="-1"><a class="header-anchor" href="#datetypetostringaslong" aria-hidden="true">#</a> dateTypeToStringAsLong</h3><p>If the incoming Row field type to be converted is <code>com.bytedance.bitsail.common.column.DateColumn</code> which is initialized by <code>java.sql.Timestamp</code>, this date column will be converted to a timestamp value.</p><h3 id="nullstringasnull" tabindex="-1"><a class="header-anchor" href="#nullstringasnull" aria-hidden="true">#</a> nullStringAsNull</h3><p>If the incoming Rowfield data to be converted is null and the target hive data type is string, the user can choose to convert it to null or an empty string &quot;&quot;.</p><table><thead><tr><th>Parameter</th><th>Condition</th><th>Converting</th></tr></thead><tbody><tr><td><code>nullStringAsNull</code></td><td><code>true</code></td><td><code>null</code> -&gt; <code>null</code></td></tr><tr><td><code>nullStringAsNull</code></td><td><code>false</code></td><td><code>null</code> -&gt; <code>&quot;&quot;</code></td></tr></tbody></table><h3 id="dateprecision" tabindex="-1"><a class="header-anchor" href="#dateprecision" aria-hidden="true">#</a> datePrecision</h3><p>This option determines the precision when converting a date column to timestamp. The optional values are &quot;SECOND&quot; and &quot; MILLISECOND&quot;.</p><p>For example, when converting &quot;2022-01-01 12:34:56&quot; to timestamp, different value will be returned according to datePrecision:</p><ul><li><code>datePrecision=SECOND</code>: 1641011696</li><li><code>datePrecision=MILLISECOND</code>: 1641011696000</li></ul><h2 id="how-to-use" tabindex="-1"><a class="header-anchor" href="#how-to-use" aria-hidden="true">#</a> how to use</h2><p>The following describes how to develop with <code>GeneralHiveExtractor</code>.</p><h3 id="initialization" tabindex="-1"><a class="header-anchor" href="#initialization" aria-hidden="true">#</a> Initialization</h3><p>After creating a <code>GeneralHiveExtractor</code> instance，the following steps are required:</p><h4 id="_1-set-column-mapping-and-field-names" tabindex="-1"><a class="header-anchor" href="#_1-set-column-mapping-and-field-names" aria-hidden="true">#</a> 1. Set <b>column mapping</b> and <b>field names</b></h4><p>fieldNames and columnMapping determine the <code>Row</code> order in which the Hive fields are written.</p><ul><li>columnMapping: The mapping of field names stored in Map form to field positions in hive.</li><li>fieldNames: Row field names in order.</li></ul><p>The following example shows how these two parameters are set:</p><ul><li>The structure of hive table to be written <span id="hive_example">hive_example</span> is:</li></ul><table><thead><tr><th>field name</th><th>data type</th><th>index</th></tr></thead><tbody><tr><td><code>field_c</code></td><td><code>STRING</code></td><td>0</td></tr><tr><td><code>field_b</code></td><td><code>STRING</code></td><td>1</td></tr><tr><td><code>field_a</code></td><td><code>STRING</code></td><td>2</td></tr></tbody></table><ul><li><code>Row</code> to convert:</li></ul><div class="language-json line-numbers-mode" data-ext="json"><pre class="language-json"><code><span class="token punctuation">[</span>
  <span class="token punctuation">{</span>
    <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;row_field_0&quot;</span><span class="token punctuation">,</span>
    <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;string&quot;</span><span class="token punctuation">,</span>
    <span class="token property">&quot;data&quot;</span><span class="token operator">:</span> <span class="token string">&quot;0&quot;</span>
  <span class="token punctuation">}</span><span class="token punctuation">,</span>
  <span class="token punctuation">{</span>
    <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;row_field_1&quot;</span><span class="token punctuation">,</span>
    <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;string&quot;</span><span class="token punctuation">,</span>
    <span class="token property">&quot;data&quot;</span><span class="token operator">:</span> <span class="token string">&quot;1&quot;</span>
  <span class="token punctuation">}</span><span class="token punctuation">,</span>
  <span class="token punctuation">{</span>
    <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;row_field_2&quot;</span><span class="token punctuation">,</span>
    <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;string&quot;</span><span class="token punctuation">,</span>
    <span class="token property">&quot;data&quot;</span><span class="token operator">:</span> <span class="token string">&quot;2&quot;</span>
  <span class="token punctuation">}</span>
<span class="token punctuation">]</span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><ul><li>According to <code>Row</code>, the <code>fieldNames</code> needs to be set as:</li></ul><div class="language-java line-numbers-mode" data-ext="java"><pre class="language-java"><code><span class="token class-name">String</span> fieldNames <span class="token operator">=</span> <span class="token punctuation">{</span><span class="token string">&quot;field_0&quot;</span><span class="token punctuation">,</span> <span class="token string">&quot;field_1&quot;</span><span class="token punctuation">,</span> <span class="token string">&quot;field_2&quot;</span><span class="token punctuation">}</span><span class="token punctuation">;</span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div></div></div><ul><li>If want to achive mapping of <code>row_field_0-&gt;field_a</code>,<code>row_field_1-&gt;field_b</code>, <code>row_field_2-&gt;field_c</code>, the <code>olumnMapping</code> needs to be set as:</li></ul><div class="language-text line-numbers-mode" data-ext="text"><pre class="language-text"><code>Map&lt;String, Integer&gt; columnMapping=ImmutableMap.of(
  &quot;row_field_0&quot;,2,
  &quot;row_field_1&quot;,1,
  &quot;row_field_2&quot;,0
  );
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h4 id="_2-set-conversion-parameters" tabindex="-1"><a class="header-anchor" href="#_2-set-conversion-parameters" aria-hidden="true">#</a> 2. Set conversion parameters</h4><p>After building a <code>ConvertToHiveObjectOptions</code>，developers need to set it to <code>GeneralWritableExtractor</code>。</p><div class="language-text line-numbers-mode" data-ext="text"><pre class="language-text"><code>ConvertToHiveObjectOptions options=ConvertToHiveObjectOptions.builder()
  .convertErrorColumnAsNull(false)
  .dateTypeToStringAsLong(false)
  .datePrecision(ConvertToHiveObjectOptions.DatePrecision.SECOND)
  .nullStringAsNull(false)
  .build();

  hiveWritableExtractor.initConvertOptions(options);
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h4 id="_3-initialize-objectinspector" tabindex="-1"><a class="header-anchor" href="#_3-initialize-objectinspector" aria-hidden="true">#</a> 3. Initialize ObjectInspector</h4><p><code>GeneralWritableExtractor</code> offers a interface to initialize ObjectInspector for converting.</p><div class="language-text line-numbers-mode" data-ext="text"><pre class="language-text"><code>public SettableStructObjectInspector createObjectInspector(final String columnNames,final String columnTypes);
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div></div></div><ul><li><code>columnNames</code>: A string consisting of field names in hive, separated by <code>&#39;,&#39;</code>, in the same order in hive.</li><li><code>columnsType</code>: A string consisting of data types in hive, separated by <code>&#39;,&#39;</code>, in the same order in hive.</li></ul><p>Take the above test table (<a href="#hive_example">hive_example</a>) as an example:</p><div class="language-text line-numbers-mode" data-ext="text"><pre class="language-text"><code>String columnNames=&quot;field_c,field_b,field_a&quot;;
  String columnTypes=&quot;string,string,string&quot;;
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="example-code" tabindex="-1"><a class="header-anchor" href="#example-code" aria-hidden="true">#</a> Example code</h3><div class="language-text line-numbers-mode" data-ext="text"><pre class="language-text"><code>/**
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
public void transformAndWrite(Serializer serDe,FileSinkOperator.RecordWriter hiveWriter)throws Exception{
  // 0. Initialize parameters.
  String[]fieldNames={&quot;field_a&quot;,&quot;field_b&quot;,&quot;field_c&quot;};
  Map&lt;String, Integer&gt; columnMapping=ImmutableMap.of(
  &quot;field_a&quot;,0,
  &quot;field_b&quot;,1,
  &quot;field_c&quot;,2
  );
  ConvertToHiveObjectOptions options=ConvertToHiveObjectOptions.builder()
  .convertErrorColumnAsNull(false)
  .dateTypeToStringAsLong(false)
  .datePrecision(ConvertToHiveObjectOptions.DatePrecision.SECOND)
  .nullStringAsNull(false)
  .build();
  String hiveColumnNames=&quot;field_0,field_1,field_2&quot;;
  String hiveColumnTypes=&quot;bigint,string,double&quot;;

  // 1. Prepare a row.
  Row row=new Row(3);
  row.setField(0,new LongColumn(100));
  row.setField(1,new StringColumn(&quot;str&quot;));
  row.setField(2,new DoubleColumn(3.14));

  // 2. Create GeneralWritableExtractor instance.
  GeneralWritableExtractor extractor=new GeneralWritableExtractor();
  extractor.setColumnMapping(columnMapping);
  extractor.setFieldNames(fieldNames);
  extractor.initConvertOptions(options);
  ObjectInspector inspector=extractor.createObjectInspector(hiveColumnNames,hiveColumnTypes);

  // 3. Transform row and write it to hive.
  Object hiveRow=extractor.createRowObject(row);
  Writable writable=serDe.serialize(hiveRow,inspector);
  hiveWriter.write(writable);
  }
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div>`,49);function m(b,h){const i=d("RouterLink");return t(),s("div",null,[u,p,e("p",null,[n("Parent document: "),o(i,{to:"/en/documents/components/conversion/introduction.html"},{default:l(()=>[n("bitsail-conversion-flink")]),_:1})]),v])}const q=a(c,[["render",m],["__file","hive-convert.html.vue"]]);export{q as default};
